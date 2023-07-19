package cluster

import com.google.common.base.Charsets
import io.netty.util.internal.ThreadLocalRandom
import org.apache.commons.lang3.RandomStringUtils
import org.slf4j.LoggerFactory
import services.scalable.index.grpc._
import services.scalable.index.impl._
import services.scalable.index.{Bytes, Commands, DefaultComparators, DefaultIdGenerators, DefaultPrinters, DefaultSerializers, IndexBuilder, QueryableIndex}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object InsertDemo {

  def main(args: Array[String]): Unit = {

    val logger = LoggerFactory.getLogger(this.getClass)

    val rand = ThreadLocalRandom.current()
    import scala.concurrent.ExecutionContext.Implicits.global

    type K = Bytes
    type V = Bytes

    val indexId = "meta"

    import services.scalable.index.DefaultComparators._

    val NUM_LEAF_ENTRIES = TestHelper.NUM_LEAF_ENTRIES
    val NUM_META_ENTRIES = TestHelper.NUM_META_ENTRIES

    implicit val idGenerator = DefaultIdGenerators.idGenerator

    implicit val cache = new DefaultCache(MAX_PARENT_ENTRIES = 80000)
    //implicit val storage = new MemoryStorage()
    implicit val storage = new CassandraStorage(TestConfig.session, false)

    val builder = IndexBuilder.create[K, V](DefaultComparators.bytesOrd, DefaultSerializers.bytesSerializer,
      DefaultSerializers.bytesSerializer)
      .storage(storage)
      .serializer(DefaultSerializers.grpcBytesBytesSerializer)
      .keyToStringConverter(DefaultPrinters.byteArrayToStringPrinter)

    val indexContext = Await.result(TestHelper.loadOrCreateIndex(IndexContext(
      TestConfig.CLUSTER_INDEX_NAME,
      NUM_LEAF_ENTRIES,
      NUM_META_ENTRIES
    )), Duration.Inf).get

    var data = Seq.empty[(K, V, Boolean)]
    val index = new QueryableIndex[K, V](indexContext)(builder)

    def insert(): Unit = {
      val n = 100 //rand.nextInt(1, 100)
      var list = Seq.empty[Tuple3[K, V, Boolean]]

      for (i <- 0 until n) {
        val k = RandomStringUtils.randomAlphanumeric(5, 10).getBytes(Charsets.UTF_8)
        val v = RandomStringUtils.randomAlphanumeric(5).getBytes(Charsets.UTF_8)

        if (!data.exists { case (k1, _, _) => bytesOrd.equiv(k, k1) } &&
          !list.exists { case (k1, _, _) => bytesOrd.equiv(k, k1) }) {
          list = list :+ (k, v, true)
        }
      }

      val cmds = Seq(
        Commands.Insert(indexId, list)
      )
      val result = Await.result(index.execute(cmds), Duration.Inf)

      //index.snapshot()

      assert(result.success)

      data = data ++ list
    }

    insert()

    logger.info(Await.result(index.save(), Duration.Inf).toString)

    val dlist = data.sortBy(_._1).map{case (k, v, _) => Tuple3(k, v, indexContext.id)}
    val ilist = Await.result(TestHelper.all(index.inOrder()), Duration.Inf)

    logger.debug(s"${Console.GREEN_B}tdata: ${dlist.map { case (k, v, _) => new String(k, Charsets.UTF_8) -> new String(v) }}${Console.RESET}\n")
    logger.debug(s"${Console.MAGENTA_B}idata: ${ilist.map { case (k, v, _) => new String(k, Charsets.UTF_8) -> new String(v) }}${Console.RESET}\n")

    assert(TestHelper.isColEqual(dlist, ilist))

    storage.close()
  }

}
