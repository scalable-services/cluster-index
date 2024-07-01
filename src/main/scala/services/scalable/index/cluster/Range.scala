package services.scalable.index.cluster

import services.scalable.index.Commands.{Command, Insert, Remove, Update}
import services.scalable.index.{Context, IndexBuilt, Leaf}
import services.scalable.index.grpc.{IndexContext, RootRef}
import ClusterResult._

import java.util.UUID
import scala.concurrent.Future
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
      case Some(block) => Some(block.asInstanceOf[Leaf[K, V]])
    }
  }

  protected def insertEmpty(leaf: Leaf[K, V], data: Seq[(K, V, Boolean)],
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

  def insert(data: Seq[Tuple3[K, V, Boolean]], insertVersion: String): Future[InsertionResult] = {
    getLeaf().flatMap {
      case None =>
        val leaf = ctx.createLeaf()
        insertEmpty(leaf, data, insertVersion)

      case Some(leaf) =>
        insertEmpty(leaf, data, insertVersion)
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
        //case cmd: Remove[K, V] => remove(cmd.keys)
        //case cmd: Update[K, V] => update(cmd.list, version)
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

}
