package cluster

import com.google.common.base.Charsets
import io.netty.util.internal.ThreadLocalRandom
import org.apache.commons.lang3.RandomStringUtils
import org.slf4j.LoggerFactory
import services.scalable.index.grpc.{DBContext, IndexContext, IndexView, RootRef}
import services.scalable.index.impl._
import services.scalable.index.{Block, Bytes, Cache, Commands, Context, DB, IdGenerator, Leaf, Meta, QueryableIndex, Serializer, Storage, Tuple}

import java.util.UUID
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

class SplitSpec extends Repeatable {

  class Range[K, V](private var ctx: DBContext, val NUM_ENTRIES: Int)(implicit val ec: ExecutionContext,
                                          val storage: Storage,
                                          val serializer: Serializer[Block[K, V]],
                                          val cache: Cache,
                                          val ord: Ordering[K],
                                          val idGenerator: IdGenerator){
    var db = new DB[K, V](ctx)
    val MIN = NUM_ENTRIES/2

    def execute(cmds: Seq[Commands.Insert[K, V]]): Future[Boolean] = {
      db.execute(cmds)
    }

    def isFull(): Boolean = {
      val index = db.findLatestIndex("main").get
      val ctx = index.ctx

      ctx.num_elements >= NUM_ENTRIES
    }

    def hasMinimum(): Boolean = {
      val index = db.findLatestIndex("main").get
      val ctx = index.ctx

      ctx.num_elements >= MIN
    }

    def save(): Future[DBContext] = {
      db.save().map { c =>
        ctx = c
        c
      }
    }

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
        val diviser = leftLink.last

        val (leftP, leftId) = leftLink.unique_id
        val leftIndexCtx = IndexContext("main", index.ctx.NUM_LEAF_ENTRIES, index.ctx.NUM_META_ENTRIES,
          Some(RootRef(leftP, leftId)), leftLink.level, leftLink.nSubtree)

        val left = new QueryableIndex[K, V](leftIndexCtx)

        db.indexes = Map("main" -> left)

        val leftDBContext = DBContext("left")
          .withLatest(IndexView.of(System.nanoTime(), Map("main" -> leftIndexCtx)))

        db.ctx = leftDBContext

        val (rightP, rightId) = rightLink.unique_id
        val rightIndexCtx = IndexContext("main", index.ctx.NUM_LEAF_ENTRIES, index.ctx.NUM_META_ENTRIES,
          Some(RootRef(rightP, rightId)), rightLink.level, rightLink.nSubtree)

        val rightDBContext = DBContext("right")
          .withLatest(IndexView.of(System.nanoTime(), Map("main" -> rightIndexCtx)))

        val right = new QueryableIndex[K, V](rightIndexCtx)
        val rightRange = new Range[K, V](rightDBContext, NUM_ENTRIES)
        rightRange.db.indexes = Map("main" -> right)

        index.ctx.blocks.foreach { case (k, b) =>
          val last = b.last

          index.ctx.blocks.remove(k)

          if(ord.lteq(last, diviser)){
            left.ctx.blocks.put(k, b)
          } else {
            right.ctx.blocks.put(k, b)
          }
        }

        rightRange
      }
    }
  }

  override val times: Int = 1

  "operations" should " run successfully" in {

    val logger = LoggerFactory.getLogger(this.getClass)

    val rand = ThreadLocalRandom.current()
    import scala.concurrent.ExecutionContext.Implicits.global

    type K = Bytes
    type V = Bytes

    import services.scalable.index.DefaultComparators._
    import services.scalable.index.DefaultSerializers._
    
    val NUM_LEAF_ENTRIES = 8//rand.nextInt(5, 64)
    val NUM_META_ENTRIES = 8//rand.nextInt(5, 64)

    implicit val idGenerator = new IdGenerator {
      override def generateId[K, V](ctx: Context[K, V]): String = UUID.randomUUID.toString
      override def generatePartition[K, V](ctx: Context[K, V]): String = "p0"
    }

    implicit val cache = new DefaultCache(MAX_PARENT_ENTRIES = 80000)
    implicit val storage = new MemoryStorage(NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
    //implicit val storage = new CassandraStorage(TestConfig.KEYSPACE, NUM_LEAF_ENTRIES, NUM_META_ENTRIES, false)

    val dbContext = DBContext("left")
    var left = new Range[K, V](dbContext, 200)

    val indexId = "main"

    left.db.createIndex(indexId, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
    //left.db.createHistory("history", NUM_LEAF_ENTRIES, NUM_META_ENTRIES)

    var data = Seq.empty[(K, V)]

    def insert(range: Range[K, V]): Unit = {
      val n = rand.nextInt(200, 400)
      var list = Seq.empty[Tuple[K, V]]

      for(i<-0 until n){
        val k = RandomStringUtils.randomAlphanumeric(5, 10).getBytes(Charsets.UTF_8)
        val v = RandomStringUtils.randomAlphanumeric(5).getBytes(Charsets.UTF_8)

        if(!data.exists{case (k1, _) => ord.equiv(k, k1)}){
          list = list :+ (k -> v)
        }
      }

      val cmds = Seq(
        Commands.Insert(indexId, list)
      )

      val result = Await.result(range.execute(cmds), Duration.Inf)

      if(result){
        data = data ++ list
      }
    }

    insert(left)

    val index = left.db.findLatestIndex("main").get

    val list = Await.result(TestHelper.all(index.inOrder()), Duration.Inf)

    logger.debug(s"${Console.GREEN_B}original left with len ${list.length}: ${list.map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v)}}${Console.RESET}\n")

    var right = Await.result(left.split(), Duration.Inf)

    insert(left)
    //insert(right)

    /*val leftCtx = Await.result(left.save(), Duration.Inf)
    val rightCtx = Await.result(right.save(), Duration.Inf)

    left = new Range[K, V](leftCtx, 200)
    right = new Range[K, V](rightCtx, 200)*/

    val leftIndex = left.db.findLatestIndex("main").get
    val leftList = Await.result(TestHelper.all(leftIndex.inOrder()), Duration.Inf)

    val rightIndex = right.db.findLatestIndex("main").get
    val rightList = Await.result(TestHelper.all(rightIndex.inOrder()), Duration.Inf)

    logger.debug(s"${Console.MAGENTA_B}left list with len ${leftList.length}: ${leftList.map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v)}}${Console.RESET}\n")
    logger.debug(s"${Console.GREEN_B}right list with len ${rightList.length}: ${rightList.map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v)}}${Console.RESET}\n")

  }

}
