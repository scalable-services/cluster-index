package services.scalable.index.cluster

import org.slf4j.{Logger, LoggerFactory}
import services.scalable.index.Commands.Command
import services.scalable.index.grpc.{IndexContext, RootRef}
import services.scalable.index.{BatchResult, IndexBuilt, Leaf, QueryableIndex}
import services.scalable.index._

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class RangeIndex[K, V](protected var meta: IndexContext)(implicit val builder: IndexBuilt[K, V]) {
  import builder._

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val ctx = Context.fromIndexContext(meta)(builder)

  def currentSnapshot(): IndexContext = {
    ctx.currentSnapshot()
  }

  def snapshot(): IndexContext = {
    ctx.snapshot()
  }

  def execute(cmds: Seq[Command[K, V]], version: String): Future[BatchResult] = {

    def execute(leaf: Leaf[K, V]): BatchResult = {
      for(cmd <- cmds){
        val result = cmd match {
          case cmd: Commands.Insert[K, V] => leaf.insert(cmd.list, version)(ctx)
          case cmd: Commands.Update[K, V] => leaf.update(cmd.list, version)(ctx)
          case cmd: Commands.Remove[K, V] => leaf.remove(cmd.keys)(ctx)
        }

        if(result.isFailure){
          return BatchResult(false, Some(result.failed.get))
        }
      }

      BatchResult(true, None)
    }

    ctx.getRoot().map(_.map(_.asInstanceOf[Leaf[K, V]])).map(_.map(_.copy()(ctx))).map {
      case None =>
        val leaf = ctx.createLeaf()

        val result = execute(leaf)

        assert(result.success)

        ctx.root = Some(leaf.unique_id)
        ctx.num_elements = leaf.length

        result

      case Some(leaf) =>
        val result = execute(leaf)

        println("isfull: ", leaf.isFull())
        assert(result.success)

        ctx.root = Some(leaf.unique_id)
        ctx.num_elements = leaf.length

        result
    }
  }

  def size(): Long = getNumElements()

  def isFull(): Boolean = {
    getNumElements() >= builder.MAX_N_ITEMS
  }

  def hasMinimum(): Boolean = {
    getNumElements() >= builder.MAX_N_ITEMS/2
  }

  /*def hasEnough(): Boolean = {
    ctx.num_elements > ctx.LEAF_MIN
  }*/

  def minNeeded(): Long = builder.MAX_N_ITEMS/2 - getNumElements()

  def canBorrowTo(target: RangeIndex[K,V]): Boolean = getNumElements() - target.minNeeded() >= builder.MAX_N_ITEMS/2

  def isEmpty(): Boolean = {
    getNumElements() == 0
  }

  def inOrder(): Future[Seq[(K, V, String)]] = {
    ctx.getRoot().map {
      case None => Seq.empty[(K, V, String)]
      case Some(leaf) => leaf.asInstanceOf[Leaf[K, V]].inOrder()
    }
  }

  def inOrderSync(): Seq[(K, V, String)] = {
    Await.result(ctx.getRoot().map {
      case None => Seq.empty[(K, V, String)]
      case Some(leaf) => leaf.asInstanceOf[Leaf[K, V]].inOrder()
    }, Duration.Inf)
  }

  def split(): Future[RangeIndex[K, V]] = {
    logger.info(s"splitting range ${ctx.indexId}...")

    ctx.getRoot().map(_.get).map(_.asInstanceOf[Leaf[K, V]].copy()(ctx)).map { leaf =>
      val right = leaf.split()(ctx)

      ctx.num_elements = leaf.length
      ctx.root = Some(leaf.unique_id)
      ctx.levels = 0
      ctx.lastChangeVersion = UUID.randomUUID().toString

      val rightContext = IndexContext()
        .withId(UUID.randomUUID().toString)
        .withRoot(RootRef(right.partition, right.id))
        .withNumElements(right.length)
        .withLevels(0)
        .withMaxNItems(meta.maxNItems)
        .withLastChangeVersion(UUID.randomUUID().toString)
        .withNumLeafItems(meta.numLeafItems)
        .withNumMetaItems(meta.numMetaItems)

      val rightRange = new RangeIndex[K, V](rightContext)
      rightRange.ctx.newBlocksReferences += right.unique_id -> right

      rightRange
    }
  }

  def merge(right: RangeIndex[K, V], version: String): Future[RangeIndex[K, V]] = {
    logger.info(s"merging range ${ctx.indexId} with ${right.ctx.indexId}...")

    for {
      leftRoot <- ctx.getRoot().map(_.get).map(_.asInstanceOf[Leaf[K, V]].copy()(ctx))
      rightRoot <- right.ctx.getRoot().map(_.get).map(_.asInstanceOf[Leaf[K, V]])
    } yield {

      val merged = leftRoot.merge(rightRoot, version)(ctx)

      ctx.num_elements = merged.length
      ctx.root = Some(merged.unique_id)
      ctx.levels = 0
      ctx.lastChangeVersion = UUID.randomUUID().toString
      ctx.num_elements = merged.length

      this
    }
  }

  def borrowLeft(range: RangeIndex[K, V]): Future[Option[RangeIndex[K, V]]] = {
    ctx.getRoot().map(_.map(_.asInstanceOf[Leaf[K, V]])).map(_.map(_.copy()(ctx))).flatMap {
      case None => Future.successful(None)
      case Some(leaf) => range.ctx.getRoot().map(_.map(_.asInstanceOf[Leaf[K, V]])).map(_.map(_.copy()(ctx)))
        .map {
          case None => None
          case Some(targetLeaf) =>
            val targetNewRoot = leaf.borrowLeftTo(targetLeaf)(range.ctx)

            ctx.root = Some(leaf.unique_id)
            ctx.num_elements = leaf.length

            range.ctx.root = Some(targetNewRoot.unique_id)
            range.ctx.num_elements = targetNewRoot.length

            Some(range)
      }
    }
  }

  def borrowRight(range: RangeIndex[K, V]): Future[Option[RangeIndex[K, V]]] = {
    ctx.getRoot().map(_.map(_.asInstanceOf[Leaf[K, V]])).map(_.map(_.copy()(ctx))).flatMap {
      case None => Future.successful(None)
      case Some(leaf) => range.ctx.getRoot().map(_.map(_.asInstanceOf[Leaf[K, V]])).map(_.map(_.copy()(ctx)))
        .map {
          case None => None
          case Some(targetLeaf) =>
            val targetNewRoot = leaf.borrowRightTo(targetLeaf)(range.ctx)

            ctx.root = Some(leaf.unique_id)
            ctx.num_elements = leaf.length

            range.ctx.root = Some(targetNewRoot.unique_id)
            range.ctx.num_elements = targetNewRoot.length

            Some(range)
        }
    }
  }
  
  def max(): Future[Option[K]] = {
    ctx.getRoot().map(_.map(_.asInstanceOf[Leaf[K, V]])).map(_.map(_.copy()(ctx))).map {
      case None => None
      case Some(leaf) => leaf.max()(ord).map(_._1)
    }
  }


  def getId(): String = {
    ctx.indexId
  }

  def getLastChangeVersion(): String = {
    ctx.lastChangeVersion
  }

  def getMaxElements(): Long = {
    builder.MAX_N_ITEMS
  }

  def getNumElements(): Long = {
    ctx.num_elements
  }

  def copy(sameId: Boolean): RangeIndex[K, V] = {
    val indexContext = ctx.currentSnapshot()
    val copy = new RangeIndex[K, V](indexContext.withId(UUID.randomUUID().toString))

    ctx.newBlocksReferences.foreach { case (k, v) =>
      copy.ctx.newBlocksReferences += k -> v
    }

    ctx.parents.foreach { case (k, v) =>
      copy.ctx.parents += k -> v
    }

    copy
  }

  def save(): Future[IndexContext] = {
    ctx.save()
  }
}

object RangeIndex {
  def fromCtx[K, V](ctx: IndexContext)(implicit builder: IndexBuilt[K, V]): RangeIndex[K, V] = {
    new RangeIndex[K, V](ctx)(builder)
  }

  def fromId[K, V](id: String)(implicit builder: IndexBuilt[K, V]): Future[RangeIndex[K, V]] = {
    import builder._

    for {
      ctx <- builder.storage.loadIndex(id).map(_.get)
      index = new RangeIndex[K, V](ctx)(builder)
    } yield {
      index
    }
  }
}
