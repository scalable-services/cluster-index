package cluster

import com.google.common.base.Charsets
import io.netty.util.internal.ThreadLocalRandom
import org.apache.commons.lang3.RandomStringUtils
import org.slf4j.LoggerFactory
import services.scalable.index.grpc.{DBContext, IndexContext, IndexView}
import services.scalable.index.impl._
import services.scalable.index.{Block, Bytes, Cache, Commands, Context, DB, IdGenerator, Leaf, Meta, QueryableIndex, Serializer, Storage, Tuple}

import java.util.UUID
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

class ClusterSpec extends Repeatable {

  class Range[K, V](var ctx: DBContext, val NUM_ENTRIES: Int)(implicit val ec: ExecutionContext,
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

    def split(): Future[Range[K, V]] = {
      val index = db.findLatestIndex("main").get

      for {
        leftRootBlock <- index.ctx.get(index.ctx.root.get)

        right = new QueryableIndex[K, V](IndexContext("main"))
        rightRootBlock = leftRootBlock.split()(right.ctx)

        leftPointer = leftRootBlock match {
          case leaf: Leaf[K, V] => leaf.unique_id
          case meta: Meta[K, V] => if(meta.length == 1) meta.pointers(0)._2.unique_id else meta.unique_id
        }

        rightPointer = rightRootBlock match {
          case leaf: Leaf[K, V] => leaf.unique_id
          case meta: Meta[K, V] => if(meta.length == 1) meta.pointers(0)._2.unique_id else meta.unique_id
        }

        (leftLink, rightLink) <- Future.sequence(Seq(index.ctx.get(leftPointer), right.ctx.get(rightPointer))).map { links =>
          (links(0), links(1))
        }
      } yield {
        val left = new QueryableIndex[K, V](IndexContext("main"))

        left.ctx.root = Some(leftPointer)
        left.ctx.num_elements = leftLink.nSubtree
        left.ctx.levels = leftLink.level

        db.indexes = db.indexes + ("main" -> left)
        val leftView = ctx.latest
        db.ctx = ctx
          .withLatest(leftView.withIndexes(
            leftView.indexes + ("main" -> left.snapshot())
          ))

        right.ctx.root = Some(rightPointer)
        right.ctx.num_elements = rightLink.nSubtree
        right.ctx.levels = rightLink.level

        val rightDBContext = DBContext("right")
        val rightRange = new Range[K, V](rightDBContext, NUM_ENTRIES)

        rightRange.db.indexes = rightRange.db.indexes + ("main" -> right)
        val rightView = rightRange.ctx.latest
        rightRange.db.ctx = rightRange.ctx
          .withLatest(rightView.withIndexes(
            rightView.indexes + ("main" -> right.snapshot())
          ))

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
      val n = 200//rand.nextInt(1, 100)
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

    val leftCtx = Await.result(left.db.save(), Duration.Inf)

    val index = left.db.findLatestIndex("main").get

    val list = Await.result(TestHelper.all(index.inOrder()), Duration.Inf)

    logger.debug(s"${Console.GREEN_B}original left: ${list.map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v)}}${Console.RESET}\n")

    /*val right = Await.result(left.split(), Duration.Inf)

    Await.result(right.db.save(), Duration.Inf)

    var leftIndex = left.db.findLatestIndex("main").get
    var rightIndex = right.db.findLatestIndex("main").get

    var leftList = Await.result(TestHelper.all(leftIndex.inOrder()), Duration.Inf)
    var rightList = Await.result(TestHelper.all(rightIndex.inOrder()), Duration.Inf)

    logger.debug(s"${Console.GREEN_B}left list: ${leftList.map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v)}}${Console.RESET}\n")
    logger.debug(s"${Console.GREEN_B}right list: ${rightList.map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v)}}${Console.RESET}\n")

    assert(leftList ++ rightList == list)

    Await.result(left.db.save(), Duration.Inf)
    Await.result(right.db.save(), Duration.Inf)*/

    val right = Await.result(left.split(), Duration.Inf)

    insert(left)

    Await.result(left.db.save(), Duration.Inf)

    val leftIndex = left.db.findLatestIndex("main").get
    val leftList = Await.result(TestHelper.all(leftIndex.inOrder()), Duration.Inf)

    logger.debug(s"${Console.MAGENTA_B}left list: ${leftList.map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v)}}${Console.RESET}\n")
  }

}
