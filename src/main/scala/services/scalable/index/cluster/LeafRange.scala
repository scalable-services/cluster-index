package services.scalable.index.cluster

import services.scalable.index.Commands.{Insert, Remove, Update}
import services.scalable.index.Errors.DUPLICATED_KEYS
import services.scalable.index.cluster.ClusterResult.{BatchResult, InsertionResult, RemovalResult, UpdateResult}
import services.scalable.index.grpc.{IndexContext, RootRef}
import services.scalable.index.{Commands, Context, IndexBuilt, Leaf, ParentInfo}

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

class LeafRange[K, V](val descriptor: IndexContext)(val builder: IndexBuilt[K, V]) extends Range[K, V] {

  import builder._

  val MIN_ITEMS = descriptor.maxNItems/2
  implicit val ctx = Context.fromIndexContext[K, V](descriptor)(builder)

  def getLeaf(): Future[Option[Leaf[K, V]]] = {
    ctx.getRoot().map(_.map(x => {
      ctx.parents.put(x.unique_id, ParentInfo())
      x.asInstanceOf[Leaf[K, V]]
    }))
  }

  def insertEmpty(data: Seq[(K, V, Boolean)], version: String): InsertionResult = {
    /*if(data.distinctBy(_._1).length < data.length) {
      return InsertionResult(false, 0, Some(new Exception("Insertion list has duplicated items!")))
    }*/

    val leaf = ctx.createLeaf()
    ctx.parents.put(leaf.unique_id, ParentInfo())

    leaf.insert(data, version) match {
      case Success(n) =>

        assert(n == data.length)

        ctx.root = Some(leaf.unique_id)
        ctx.num_elements = leaf.length

        InsertionResult(true, n, None)

      case Failure(ex) => InsertionResult(false, 0, Some(ex))
    }
  }

  def insertLeaf(leaf: Leaf[K, V], data: Seq[(K, V, Boolean)], version: String): InsertionResult = {

    if(leaf.length + data.length > descriptor.maxNItems){
      assert(false)
    }

    val filter = data.groupBy(_._1).filter(_._2.length > 1).toSeq

    if(!filter.isEmpty) {
      return InsertionResult(false, 0, Some(new DUPLICATED_KEYS[K, V](filter.map(_._1), ctx.builder.ks)))
    }

    val copy = leaf.copy()
    ctx.parents.put(copy.unique_id, ParentInfo())

    copy.insert(data, version) match {
      case Success(n) =>

        assert(n == data.length)

        ctx.root = Some(copy.unique_id)
        ctx.num_elements = copy.length

        InsertionResult(true, n, None)

      case Failure(ex) => InsertionResult(false, 0, Some(ex))
    }
  }

  def insert(data: Seq[(K, V, Boolean)], version: String): Future[InsertionResult] = {
    getLeaf().map {
      case None => insertEmpty(data, version)
      case Some(leaf) => insertLeaf(leaf, data, version)
    }
  }

  private def remove(keys: Seq[(K, Option[String])], version: String): Future[RemovalResult] = {
    getLeaf().map {
      case None => RemovalResult(false, 0, Some(new Exception("Empty range!")))
      case Some(leaf) =>

        val copy = leaf.copy()
        ctx.parents.put(copy.unique_id, ParentInfo())

        copy.remove(keys)(ctx) match {
          case Success(n) =>
            assert(n == keys.length)

            if(copy.isEmpty()){
              ctx.root = None
              ctx.num_elements = 0
            } else {
              ctx.root = Some(copy.unique_id)
              ctx.num_elements = copy.length
            }

            RemovalResult(true, n, None)

          case Failure(ex) => RemovalResult(false, 0, Some(ex))
        }

    }
  }

  private def update(data: Seq[(K, V, Option[String])], version: String): Future[UpdateResult] = {
    getLeaf().map {
      case None => UpdateResult(false, 0, Some(new Exception("Empty range!")))
      case Some(leaf) =>

        val copy = leaf.copy()
        ctx.parents.put(copy.unique_id, ParentInfo())

        copy.update(data, version)(ctx) match {
          case Success(n) =>

            assert(n == data.length)

            ctx.root = Some(copy.unique_id)
            ctx.num_elements = copy.length

            UpdateResult(true, n, None)

          case Failure(ex) => UpdateResult(false, 0, Some(ex))
        }

    }
  }

  override def execute(commands: Seq[Commands.Command[K, V]], version: String): Future[ClusterResult.BatchResult] = {

    def process(pos: Int, error: Option[Throwable], n: Int): Future[BatchResult] = {
      if(error.isDefined) {
        return Future.successful(BatchResult(false, error))
      }

      if(pos == commands.length) {
        return Future.successful(BatchResult(true, None, n))
      }

      val cmd = commands(pos)

      (cmd match {
        case cmd: Insert[K, V] => insert(cmd.list, cmd.version.getOrElse(version))
        case cmd: Remove[K, V] => remove(cmd.keys, cmd.version.getOrElse(version))
        case cmd: Update[K, V] => update(cmd.list, cmd.version.getOrElse(version))
      }).flatMap(prev => process(pos + 1, prev.error, prev.n))
    }

    process(0, None, 0)
  }

  override def inOrder(): Future[Seq[(K, V, String)]] = {
    getLeaf().map {
      case None => Seq.empty[(K, V, String)]
      case Some(leaf) => leaf.inOrder()
    }
  }

  override def inOrderSync(): Seq[(K, V, String)] = {
    Await.result(inOrder(), Duration.Inf)
  }

  override def isEmpty(): Future[Boolean] = {
    getLeaf().map {
      case None => true
      case Some(leaf) => leaf.isEmpty()
    }
  }

  override def isFull(): Future[Boolean] = {
    getLeaf().map {
      case None => true
      case Some(leaf) =>

        //println(leaf.unique_id, leaf.MAX, leaf.isFull(), "len ", leaf.length)

        leaf.isFull()
    }
  }

  override def hasMinimum(): Future[Boolean] = {
    getLeaf().map {
      case None => false
      case Some(leaf) => leaf.hasMinimum()
    }
  }

  override def hasEnough(): Future[Boolean] = {
    getLeaf().map {
      case None => false
      case Some(leaf) => leaf.hasEnough()
    }
  }

  override def save(): Future[IndexContext] = {
    ctx.save()
  }

  override def borrow(target: Range[K, V]): Future[Range[K, V]] = {
    val t = target.asInstanceOf[LeafRange[K, V]]

    assert(t.ctx.num_elements < t.builder.MAX_N_ITEMS/2)

    val missingN = t.missingToMin()

    if(ctx.num_elements - missingN < builder.MAX_N_ITEMS/2){
      assert(false)
    }

    getLeaf().map(_.get).map(_.copy()).flatMap { thisLeaf =>
      t.getLeaf().map(_.get).map(_.copy()).map { targetLeaf =>
        thisLeaf.borrow(targetLeaf)

        ctx.root = Some(thisLeaf.unique_id)
        ctx.num_elements = thisLeaf.length

        t.ctx.root = Some(targetLeaf.unique_id)
        t.ctx.num_elements = targetLeaf.length

        ctx.newBlocksReferences.foreach { case (bid, b) =>
          t.ctx.newBlocksReferences.put(bid, b)
        }

        assert(ctx.num_elements == Await.result(length(), Duration.Inf))
        assert(t.ctx.num_elements == Await.result(t.length(), Duration.Inf))

        assert(!Await.result(isEmpty(), Duration.Inf))

        this
      }
    }
  }

  protected def setMissingParent(x: Leaf[K, V]): Leaf[K, V] = {
    ctx.parents.put(x.unique_id, ParentInfo())
    x
  }

  override def merge(block: Range[K, V], version: String): Future[Range[K, V]] = {
    val b = block.asInstanceOf[LeafRange[K, V]]

    assert(b.ctx.num_elements > 0)

    getLeaf().map(_.get).map(setMissingParent).map(_.copy()).flatMap { thisLeaf =>
      b.getLeaf().map(_.get).map(setMissingParent).map { blockLeaf =>
        thisLeaf.merge(blockLeaf, version)

        ctx.root = Some(thisLeaf.unique_id)
        ctx.num_elements = thisLeaf.length

        b.ctx.root = Some(blockLeaf.unique_id)
        b.ctx.num_elements = blockLeaf.length

        assert(ctx.num_elements == Await.result(length(), Duration.Inf))
        assert(b.ctx.num_elements == Await.result(b.length(), Duration.Inf))

        this
      }
    }
  }

  override def split(): Future[Range[K, V]] = {
    getLeaf().map(_.get).map(_.copy()).map { thisLeaf =>
      val rightLeaf = thisLeaf.split()

      ctx.root = Some(thisLeaf.unique_id)
      ctx.num_elements = thisLeaf.length

      val right = new LeafRange[K, V](descriptor
        .withId(UUID.randomUUID().toString)
        .withNumElements(rightLeaf.length)
        .withRoot(RootRef(rightLeaf.partition, rightLeaf.id))
        .withLastChangeVersion(UUID.randomUUID().toString))(builder)

      right.ctx.root = Some(rightLeaf.unique_id)
      right.ctx.num_elements = rightLeaf.length

      ctx.newBlocksReferences.foreach { case (bid, b) =>
        right.ctx.newBlocksReferences.put(bid, b)
      }

      println(s"left splitting: ${ctx.indexId}, right splitting: ", right.ctx.indexId)

      right
    }
  }

  override def copy(sameId: Boolean = false): Future[Range[K, V]] = {
    val copy = new LeafRange[K, V](ctx.currentSnapshot())(builder)

    ctx.newBlocksReferences.foreach { case (bid, b) =>
      copy.ctx.newBlocksReferences.put(bid, b)
    }

    Future.successful(copy)
  }

  override def max(): Future[Option[(K, V, String)]] = {
    getLeaf().map {
      case None => None
      case Some(leaf) => leaf.max()
    }
  }

  override def min(): Future[Option[(K, V, String)]] = {
    getLeaf().map {
      case None => None
      case Some(leaf) => leaf.min()
    }
  }

  override def length(): Future[Long] = {
    getLeaf().map {
      case None => 0L
      case Some(leaf) => leaf.length
    }
  }

  override def canBorrow(r: Range[K, V]): Boolean = {
    (ctx.num_elements - r.asInstanceOf[LeafRange[K, V]].missingToMin()) >= builder.MAX_N_ITEMS/2
  }

  override def missingToMin(): Int = {
    (builder.MAX_N_ITEMS/2 - ctx.num_elements).toInt
  }

  override def canMerge(r: Range[K, V]): Boolean = {
    ctx.num_elements + r.asInstanceOf[LeafRange[K, V]].ctx.num_elements <= builder.MAX_N_ITEMS
  }
}
