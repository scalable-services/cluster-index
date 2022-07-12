package cluster

import com.google.common.base.Charsets
import io.netty.util.internal.ThreadLocalRandom
import org.apache.commons.lang3.RandomStringUtils
import org.slf4j.LoggerFactory
import services.scalable.index.grpc.{DBContext, IndexView}
import services.scalable.index.impl._
import services.scalable.index.{Bytes, Commands, Context, IdGenerator, Tuple}

import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class RangeSpec extends Repeatable {

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
