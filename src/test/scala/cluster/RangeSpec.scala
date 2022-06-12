package cluster

import com.google.common.base.Charsets
import io.netty.util.internal.ThreadLocalRandom
import org.apache.commons.lang3.RandomStringUtils
import org.slf4j.LoggerFactory
import services.scalable.index.grpc.DBContext
import services.scalable.index.impl._
import services.scalable.index.{Block, Bytes, Cache, Commands, Context, DB, IdGenerator, Leaf, Meta, Serializer, Storage, Tuple}

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class RangeSpec extends Repeatable {

  class Range[K, V](var ctx: DBContext, val NUM_ENTRIES: Int)(implicit val ec: ExecutionContext,
                                          val storage: Storage,
                                          val serializer: Serializer[Block[K, V]],
                                          val cache: Cache,
                                          val ord: Ordering[K],
                                          val idGenerator: IdGenerator){
    val db = new DB[K, V](ctx)
    val MIN = NUM_ENTRIES/2

    def execute(cmds: Seq[Commands.Insert[K, V]]): Future[Boolean] = {
      db.execute(cmds)
    }

    def split(): Future[Range[K, V]] = {
      val index = db.indexes("main")
      implicit val indexCtx = index.ctx
      val rootId = indexCtx.root.get

      def splitMeta(block: Meta[K, V]): Range[K, V] = {
        val right = block.split()

        val newRoot = indexCtx.createMeta()

        val leftPointer = if(block.length == 1) block.pointers(0)._2 else block.unique_id
        val rightPointer = if(right.length == 1) right.pointers(0)._2 else right.unique_id

        /*newRoot.insert(Seq(
          block.last -> leftPointer,
          right.last -> rightPointer
        ))*/

        indexCtx.root = Some(block.unique_id)

        val rightDBCtx = DBContext()
        val rightRange = new Range[K, V](rightDBCtx, NUM_ENTRIES)

        rightRange.db.createIndex("main", index.ctx.NUM_LEAF_ENTRIES, index.ctx.NUM_META_ENTRIES)
        rightRange.db.createIndex("history", index.ctx.NUM_LEAF_ENTRIES, index.ctx.NUM_META_ENTRIES)

        val rightIndex = rightRange.db.indexes("main")
        val rightCtx = rightIndex.ctx

        rightCtx.root = Some(right.unique_id)

        //rightCtx.blocks = this.db.indexes("main").ctx.

        //rightCtx.num_elements = right.length
        //rightCtx.levels = indexCtx.levels - 1

        rightRange
      }

      indexCtx.get(rootId).map { root =>
        root match {
          case leaf: Leaf[K, V] => null
          case meta: Meta[K, V] => splitMeta(meta)
        }
      }
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

    var db = new DB[K, V]()

    val indexId = "main"

    db.createIndex(indexId, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
    db.createHistory("history", NUM_LEAF_ENTRIES, NUM_META_ENTRIES)

    var data = Seq.empty[(K, V)]

    val range = new Range[K, V](db.ctx, 200)

    def insert(): Unit = {
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

    insert()

    db = range.db

    Await.result(db.save(), Duration.Inf)

    println("isFull ", range.isFull())

    if(range.isFull()){

      val list = Await.result(TestHelper.all(range.db.indexes("main").inOrder()), Duration.Inf)

      logger.debug(s"${Console.MAGENTA_B}left: ${list.map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v)}}${Console.RESET}\n")

      val right = Await.result(range.split(), Duration.Inf)

      val leftIndex = range.db.findLatestIndex("main").get
      val rightIndex = right.db.findLatestIndex("main").get

      val leftList = Await.result(TestHelper.all(leftIndex.inOrder()), Duration.Inf)
      val rightList = Await.result(TestHelper.all(rightIndex.inOrder()), Duration.Inf)

      val mergedList = leftList ++ rightList

      logger.debug(s"${Console.GREEN_B}left: ${leftList.map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v)}}${Console.RESET}\n")
      logger.debug(s"${Console.RED_B}right: ${rightList.map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v)}}${Console.RESET}\n")

      assert(TestHelper.isColEqual(mergedList, list))
    }

    /*val index = db.findLatestIndex("main").get

    val list = Await.result(TestHelper.all(index.inOrder()), Duration.Inf)

    logger.debug(s"${Console.GREEN_B}tdata: ${list.map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v)}}${Console.RESET}\n")

    logger.info(s"${Console.YELLOW_B}n: ${index.ctx.num_elements} levels: ${index.ctx.levels}${Console.RESET}")*/

  }

}
