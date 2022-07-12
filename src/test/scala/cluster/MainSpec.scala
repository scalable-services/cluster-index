package cluster

import com.google.common.base.Charsets
import io.netty.util.internal.ThreadLocalRandom
import org.apache.commons.lang3.RandomStringUtils
import org.slf4j.LoggerFactory
import services.scalable.index.grpc.{DBContext, IndexView}
import services.scalable.index.{Block, Bytes, Commands, Context, IdGenerator, QueryableIndex, Serializer, Tuple}
import services.scalable.index.impl._
import com.google.protobuf.any.Any
import services.scalable.index.impl._

import java.util.UUID
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

class MainSpec extends Repeatable {

  override val times: Int = 1

  "operations" should " run successfully" in {

    val logger = LoggerFactory.getLogger(this.getClass)

    val rand = ThreadLocalRandom.current()
    import scala.concurrent.ExecutionContext.Implicits.global

    type K = Bytes
    type V = Bytes

    import services.scalable.index.DefaultComparators._
    import services.scalable.index.DefaultSerializers._

    val NUM_LEAF_ENTRIES = 8
    val NUM_META_ENTRIES = 8

    implicit val idGenerator = new IdGenerator {
      override def generateId[K, V](ctx: Context[K, V]): String = UUID.randomUUID.toString
      override def generatePartition[K, V](ctx: Context[K, V]): String = "p0"
    }

    implicit val cache = new DefaultCache(MAX_PARENT_ENTRIES = 80000)
    implicit val storage = new MemoryStorage(NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
    //implicit val storage = new CassandraStorage(TestConfig.KEYSPACE, NUM_LEAF_ENTRIES, NUM_META_ENTRIES, false)

    /*implicit val dbCtxBlockSerializer = new Serializer[Block[Bytes, DBContext]] {
      override def serialize(t: Block[Bytes, DBContext]): Bytes = grpcDBContextSerializer.serialize(t)
      override def deserialize(b: Bytes): Block[Bytes, DBContext] = grpcDBContextSerializer.deserialize(b)
    }*/

    val MAX_ENTRIES = 200

    val cluster = new ClusterIndex[K, V](DBContext("clusterIndex"), MAX_ENTRIES,
      NUM_LEAF_ENTRIES, NUM_META_ENTRIES)

    val n = 1000
    var data = Seq.empty[(K, V)]

    for(i<-0 until n){
      val k = RandomStringUtils.randomAlphanumeric(5, 10).getBytes(Charsets.UTF_8)
      val v = RandomStringUtils.randomAlphanumeric(5).getBytes(Charsets.UTF_8)

      if(!data.exists{case (k1, _) => ord.equiv(k, k1)}){
        data = data :+ (k -> v)
      }
    }

    val result = Await.result(cluster.insert(data), Duration.Inf)

    //Await.result(cluster.save(), Duration.Inf)

    val first = cluster.meta.findLatestIndex("main").get

    val rangesList = Await.result(TestHelper.all(first.inOrder()), Duration.Inf).map{case (k, c) => new String(k) -> c}

    println(s"\n${Console.GREEN_B}result with len ${rangesList.length}: ${rangesList.map{case (k, c) => new String(k) ->
      c.latest.indexes.head._2}}${Console.RESET}\n")

    // ISSUE: context id for Index is not the same in DB class
    def printRange(c: DBContext): Seq[(K, V)] = {
      val range = new Range[K, V](c, MAX_ENTRIES)
      val index = range.db.findLatestIndex("main").get

      val list = Await.result(TestHelper.all(index.inOrder()), Duration.Inf)

      list
    }

    val elements = rangesList.map{case (k, c) => printRange(c)}.flatten

    val list = data.sortBy(_._1)

    logger.debug(s"${Console.GREEN_B}list with len ${list.length}: ${list.map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v)}}${Console.RESET}\n")
    logger.debug(s"${Console.MAGENTA_B}left list with len ${elements.length}: ${elements.map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v)}}${Console.RESET}\n")

  }

}
