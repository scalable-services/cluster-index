package cluster

import cluster.ClusterSerializers._
import cluster.grpc._
import com.google.common.base.Charsets
import com.google.protobuf.ByteString
import org.apache.commons.lang3.RandomStringUtils
import org.slf4j.LoggerFactory
import services.scalable.index.grpc.{IndexContext, KVPair}
import services.scalable.index.impl.{CassandraStorage, DefaultCache}
import services.scalable.index.{Bytes, Commands, Context, DefaultComparators, DefaultPrinters, DefaultSerializers, IdGenerator, IndexBuilder}

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Main2 {

  def main(args: Array[String]): Unit = {

    val logger = LoggerFactory.getLogger(this.getClass)

    val rand = ThreadLocalRandom.current()
    import scala.concurrent.ExecutionContext.Implicits.global

    type K = Bytes
    type V = Bytes

    import services.scalable.index.DefaultComparators._

    val indexId = TestConfig.CLUSTER_INDEX_NAME //UUID.randomUUID().toString

    implicit val idGenerator = new IdGenerator {
      override def generateId[K, V](ctx: Context[K, V]): String = UUID.randomUUID.toString
      override def generatePartition[K, V](ctx: Context[K, V]): String = "p0"
    }

    implicit val cache = new DefaultCache(MAX_PARENT_ENTRIES = 80000)
    //implicit val storage = new MemoryStorage()
    implicit val storage = new CassandraStorage(TestConfig.session, false)

    val indexBuilder = IndexBuilder.create[K, V](DefaultComparators.bytesOrd)
      .storage(storage)
      .serializer(DefaultSerializers.grpcBytesBytesSerializer)
      .keyToStringConverter(DefaultPrinters.byteArrayToStringPrinter)

    val clusterMetaBuilder = IndexBuilder.create[K, KeyIndexContext](DefaultComparators.bytesOrd)
      .storage(storage)
      .serializer(grpcByteArrayKeyIndexContextSerializer)
      .keyToStringConverter(DefaultPrinters.byteArrayToStringPrinter)

    val metaContext = Await.result(TestHelper.loadOrCreateIndex(IndexContext(
      indexId,
      TestHelper.NUM_LEAF_ENTRIES,
      TestHelper.NUM_META_ENTRIES
    ).withMaxNItems(Int.MaxValue)), Duration.Inf).get

    var data = Seq.empty[(K, V, Boolean)]
    //val meta = new QueryableIndex[K, V](metaContext)

    def insert(start: Int, end: Int): Seq[Tuple3[K, V, Boolean]] = {
       //rand.nextInt(1, 100)
      var list = Seq.empty[Tuple3[K, V, Boolean]]

      for (i <- start until end) {
        val k = //i.toString.getBytes(Charsets.UTF_8)
          RandomStringUtils.randomAlphanumeric(5, 10).getBytes(Charsets.UTF_8)
        val v = RandomStringUtils.randomAlphanumeric(5).getBytes(Charsets.UTF_8)

        if (!data.exists { case (k1, _, _) => bytesOrd.equiv(k, k1) } &&
          !list.exists { case (k1, _, _) => bytesOrd.equiv(k, k1) }) {
          list = list :+ (k, v, true)
        }
      }

      data ++= list

      list
    }

    var list = insert(0, 3333)
    list = list.sortBy(_._1)

    val cindex = new ClusterIndex[K, V](
        metaContext
        .withId(indexId)
        .withMaxNItems(Int.MaxValue)
        .withLevels(0)
        .withNumLeafItems(TestHelper.NUM_LEAF_ENTRIES)
        .withNumMetaItems(TestHelper.NUM_META_ENTRIES),
      TestHelper.MAX_ITEMS,
      TestHelper.NUM_LEAF_ENTRIES,
      TestHelper.NUM_META_ENTRIES
    )(indexBuilder, clusterMetaBuilder)

    val savedMetaContext = Await.result(cindex.insert(list), Duration.Inf)

    val elements = cindex.inOrder().toList

    logger.info(s"${Console.YELLOW_B}list data:     ${list.toList.map { case (k, v, _) => new String(k, Charsets.UTF_8) }}${Console.RESET}\n")
    logger.info(s"${Console.YELLOW_B}elements data: ${elements.map { case (k, v, _) => new String(k, Charsets.UTF_8) }}${Console.RESET}\n")

    Await.result(storage.close(), Duration.Inf)
  }

}
