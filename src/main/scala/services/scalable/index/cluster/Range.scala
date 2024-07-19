package services.scalable.index.cluster

import services.scalable.index.Commands.{Command, Insert, Remove, Update}
import services.scalable.index.{Context, Errors, IndexBuilt, Leaf, ParentInfo}
import services.scalable.index.grpc.{IndexContext, RootRef}
import ClusterResult._

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

class Range[K, V](descriptor: IndexContext)(val builder: IndexBuilt[K, V]) {

  import builder._

  implicit var ctx: Context[K, V] = Context.fromIndexContext(descriptor)(builder)

  def save(): Future[IndexContext] = {
    storage.loadOrCreate(ctx.currentSnapshot()).flatMap { _ =>
      ctx.save()
    }
  }

  protected def getLeaf(): Future[Option[Leaf[K, V]]] = {
    ctx.getRoot().map {
      case None => None
      case Some(block) =>
        ctx.parents.put(block.unique_id, ParentInfo())
        Some(block.asInstanceOf[Leaf[K, V]])
    }
  }

  protected def insert(leaf: Leaf[K, V], data: Seq[(K, V, Boolean)],
                            insertVersion: String): Future[InsertionResult] = {
    leaf.insert(data, insertVersion) match {
      case Success(n) =>

        ctx.root = Some(leaf.unique_id)
        ctx.num_elements += n
        ctx.levels = 1

        Future.successful(InsertionResult(true, n, None))
      case Failure(ex) => Future.successful(InsertionResult(false, 0, Some(ex)))
    }
  }

  def split(): Future[Range[K, V]] = {
    getLeaf().map(_.get).map { leaf =>

      val descriptor = ctx.currentSnapshot()
      val right = leaf.split()

      ctx.num_elements = leaf.length
      ctx.root = Some(leaf.unique_id)

      val range = new Range[K, V](
        descriptor
          .withId(UUID.randomUUID.toString)
          .withRoot(RootRef(right.partition, right.id))
          .withNumElements(right.length)
      )(builder)

      ctx.parents.remove(right.unique_id)
      ctx.newBlocksReferences.remove(right.unique_id)

      range.ctx.newBlocksReferences.put(right.unique_id, right)

      range
    }
  }

  protected def insert(data: Seq[Tuple3[K, V, Boolean]], insertVersion: String): Future[InsertionResult] = {
    getLeaf().flatMap {
      case None =>
        val leaf = ctx.createLeaf()
        insert(leaf, data, insertVersion)

      case Some(leaf) =>
        insert(leaf, data, insertVersion)
    }
  }

  protected def remove(keys: Seq[Tuple2[K, Option[String]]], removalVersion: String): Future[RemovalResult] = {
    getLeaf().flatMap {
      case None => Future.failed(new RuntimeException("no range!"))
      case Some(leaf) =>

        val result = leaf.remove(keys)

        if(!result.isSuccess){
          assert(false)
        }

        ctx.root = Some(leaf.unique_id)
        ctx.num_elements -= keys.length
        ctx.parents.put(leaf.unique_id, ParentInfo())
        ctx.levels = 1

        Future.successful(RemovalResult(result.isSuccess, if(result.isSuccess) result.get else 0,
          if(result.isSuccess) None else Some(result.failed.get)))
    }
  }

  protected def update(data: Seq[Tuple3[K, V, Option[String]]], updateVersion: String): Future[UpdateResult] = {
    getLeaf().flatMap {
      case None => Future.failed(new RuntimeException("no range!"))
      case Some(leaf) =>

        val beforeList = leaf.inOrder()

        val result = leaf.update(data, updateVersion)

        val afterList = leaf.inOrder()

        val map1 = beforeList.map(x => x._1 -> x._2).toMap
        val map2 = afterList.map(x => x._1 -> x._2).toMap

        if(!result.isSuccess){
          assert(false)
        }

        Future.successful(UpdateResult(result.isSuccess, if(result.isSuccess) result.get else 0,
          if(result.isSuccess) None else Some(result.failed.get)))
    }
  }

  def execute(cmds: Seq[Command[K, V]], version: String = ctx.id): Future[BatchResult] = {

    def process(pos: Int, error: Option[Throwable], n: Int): Future[BatchResult] = {
      if(error.isDefined) {
        return Future.successful(BatchResult(false, error))
      }

      if(pos == cmds.length) {
        return Future.successful(BatchResult(true, None, n))
      }

      val cmd = cmds(pos)

      (cmd match {
        case cmd: Insert[K, V] => insert(cmd.list, cmd.version.getOrElse(version))
        case cmd: Remove[K, V] => remove(cmd.keys, cmd.version.getOrElse(version))
        case cmd: Update[K, V] => update(cmd.list, cmd.version.getOrElse(version))
      }).flatMap(prev => process(pos + 1, prev.error, prev.n))
    }

    process(0, None, 0)
  }

  def length: Future[Int] = {
    getLeaf().map {
      case None => 0
      case Some(leaf) => leaf.length
    }
  }

  def isFull(): Future[Boolean] = {
    getLeaf().map {
      case None => false
      case Some(leaf) => leaf.isFull()
    }
  }

  def isEmpty(): Future[Boolean] = {
    getLeaf().map {
      case None => true
      case Some(leaf) => leaf.isEmpty()
    }
  }

  def hasEnough(): Future[Boolean] = {
    getLeaf().map {
      case None => false
      case Some(leaf) => leaf.hasEnough()
    }
  }

  def hasMinimum(): Future[Boolean] = {
    getLeaf().map {
      case None => false
      case Some(leaf) => leaf.hasMinimum()
    }
  }

  def inOrder(): Future[Seq[Tuple3[K, V, String]]] = {
    getLeaf().map {
      case None => Seq.empty[Tuple3[K, V, String]]
      case Some(leaf) => leaf.inOrder()
    }
  }

  def inOrderSync(): Seq[Tuple3[K, V, String]] = {
    Await.result(inOrder(), Duration.Inf)
  }

  def max(): Future[Option[Tuple3[K, V, String]]] = {
    getLeaf().map {
      case None => None
      case Some(leaf) => leaf.max()
    }
  }

  def copy(sameId: Boolean = false): Future[Range[K, V]] = {
    getLeaf().map {
      case None =>
        val descriptor = ctx.currentSnapshot()
        new Range[K, V](descriptor.withId(UUID.randomUUID().toString))(builder)

      case Some(leaf) =>
        val descriptor = ctx.currentSnapshot()

        val copy = leaf.copy()

        val range = new Range[K, V](
              descriptor
                .withId(if(sameId) descriptor.id else UUID.randomUUID.toString)
                .withRoot(RootRef(copy.partition, copy.id))
          )(builder)

        ctx.parents.remove(copy.unique_id)
        ctx.newBlocksReferences.remove(copy.unique_id)

        range.ctx.newBlocksReferences.put(copy.unique_id, copy)

        range
    }
  }

  def merge(right: Range[K, V], version: String): Future[Range[K, V]] = {
    for {
      leftLeaf <- getLeaf().map(_.get)
      rightLeaf <- right.getLeaf().map(_.get)
    } yield {

      assert(leftLeaf.length + rightLeaf.length <= builder.MAX_N_ITEMS)

      val before = (leftLeaf.inOrder() ++ rightLeaf.inOrder()).map(_._1).sorted.toList
      val merged = leftLeaf.merge(rightLeaf, version).asInstanceOf[Leaf[K, V]]

      ctx.num_elements = merged.length
      ctx.lastChangeVersion = UUID.randomUUID().toString
      ctx.levels = 1
      ctx.root = Some(merged.unique_id)
      ctx.parents.put(merged.unique_id, ParentInfo())

      val after = merged.inOrder().map(_._1).toList

      if(before != after){
        assert(false)
      }

      this
    }
  }

  def borrow(target: Range[K, V], version: String): Future[Range[K, V]] = {

    assert(target.ctx.num_elements < builder.MAX_N_ITEMS/2)

    for {
      leftLeaf <- getLeaf().map(_.get)
      targetLeaf <- target.getLeaf().map(_.get)
    } yield {

      val before = (leftLeaf.inOrder() ++ targetLeaf.inOrder()).sortBy(_._1).map(_._1)

      val after = if(builder.ord.gt(leftLeaf.min().get._1, targetLeaf.max().get._1)){
        leftLeaf.borrowRightTo(targetLeaf)
        (targetLeaf.inOrder() ++ leftLeaf.inOrder()).map(_._1).toSeq
      } else {
        leftLeaf.borrowLeftTo(targetLeaf)
        (leftLeaf.inOrder() ++ targetLeaf.inOrder()).map(_._1).toSeq
      }

      val lv = UUID.randomUUID().toString

      ctx.num_elements = leftLeaf.length
      ctx.lastChangeVersion = lv
      ctx.levels = 1
      ctx.root = Some(leftLeaf.unique_id)
      ctx.parents.put(leftLeaf.unique_id, ParentInfo())

      target.ctx.num_elements = targetLeaf.length
      target.ctx.lastChangeVersion = lv
      target.ctx.levels = 1
      target.ctx.root = Some(targetLeaf.unique_id)
      target.ctx.parents.put(targetLeaf.unique_id, ParentInfo())

      val fnoteq = before.zipWithIndex.filterNot {case (k, idx) => ord.equiv(k, after(idx))}

      if(!(before.length == after.length && fnoteq.isEmpty)){
        assert(false)
      }

      this
    }
  }

  def missingToMin(): Int = {
    (builder.MAX_N_ITEMS/2 - ctx.num_elements).toInt
  }

  def canBorrow(n: Int): Future[Boolean] = {
    getLeaf().map {
      case None => false
      case Some(leaf) => leaf.length - n >= leaf.MIN
    }
  }

}
