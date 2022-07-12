package cluster

import services.scalable.index.{Block, Cache, Commands, Context, DB, IdGenerator, Leaf, Meta, QueryableIndex, Serializer, Storage}
import services.scalable.index.grpc.{DBContext, IndexContext, IndexView, RootRef}

import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

class Range[K, V](private var dbCtx: DBContext, val NUM_ENTRIES: Int)(implicit val ec: ExecutionContext,
                                                                    //val dbcontext: Context[K, DBContext],
                                                                    val storage: Storage,
                                                                    val serializer: Serializer[Block[K, V]],
                                                                    val cache: Cache,
                                                                    val ord: Ordering[K],
                                                                    val idGenerator: IdGenerator){
  var db = new DB[K, V](dbCtx)
  val MIN = NUM_ENTRIES/2

  def execute(cmds: Seq[Commands.Insert[K, V]]): Future[Boolean] = {
    db.execute(cmds)
  }

  def isFull(): Boolean = {
    val index = db.findLatestIndex("main").get
    index.ctx.num_elements >= NUM_ENTRIES
  }

  def hasMinimum(): Boolean = {
    val index = db.findLatestIndex("main").get
    index.ctx.num_elements >= MIN
  }

  def save(): Future[DBContext] = {
    db.save().map { c =>
      dbCtx = c
      c
    }
  }

  // TO DO: history tracking...
  def split(): Future[Range[K, V]] = {
    val index = db.findLatestIndex("main").get

    for {
      leftRootBlock <- index.ctx.get(index.ctx.root.get)
      rightRootBlock = leftRootBlock.split()(index.ctx)

      leftPointer = leftRootBlock match {
        case leaf: Leaf[K, V] => leaf.unique_id
        case meta: Meta[K, V] => if(meta.length == 1) meta.pointers(0)._2.unique_id else meta.unique_id
      }

      rightPointer = rightRootBlock match {
        case leaf: Leaf[K, V] => leaf.unique_id
        case meta: Meta[K, V] => if(meta.length == 1) meta.pointers(0)._2.unique_id else meta.unique_id
      }

      (leftLink, rightLink) <- Future.sequence(Seq(index.ctx.get(leftPointer), index.ctx.get(rightPointer))).map { links =>
        (links(0), links(1))
      }
    } yield {

      println(leftLink.unique_id, rightLink.unique_id)

      // To check the subtrees...
      val divider = leftLink.last

      val (leftP, leftId) = leftLink.unique_id
      val leftIndexCtx = IndexContext("main", index.ctx.NUM_LEAF_ENTRIES, index.ctx.NUM_META_ENTRIES,
        Some(RootRef(leftP, leftId)), leftLink.level, leftLink.nSubtree)

      val left = new QueryableIndex[K, V](leftIndexCtx)

      db.indexes = TrieMap("main" -> left)

      val leftDBContext = DBContext(dbCtx.id)
        .withLatest(IndexView.of(System.nanoTime(), Map("main" -> leftIndexCtx)))

      db.ctx = leftDBContext

      val (rightP, rightId) = rightLink.unique_id
      val rightIndexCtx = IndexContext("main", index.ctx.NUM_LEAF_ENTRIES, index.ctx.NUM_META_ENTRIES,
        Some(RootRef(rightP, rightId)), rightLink.level, rightLink.nSubtree)

      val rightDBContext = DBContext(UUID.randomUUID().toString)
        .withLatest(IndexView.of(System.nanoTime(), Map("main" -> rightIndexCtx)))

      val right = new QueryableIndex[K, V](rightIndexCtx)
      val rightRange = new Range[K, V](rightDBContext, NUM_ENTRIES)
      //rightRange.db.indexes = Map("main" -> right)

      db.indexes = TrieMap("main" -> right)

      index.ctx.getBlocks().foreach { case (_, b) =>
        val last = b.last

        if(ord.lteq(last, divider)){
          index.ctx.put(b)
        } else {
          right.ctx.put(b)
        }
      }

      rightRange
    }
  }
}
