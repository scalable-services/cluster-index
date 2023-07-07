package cluster

import cluster.Serializers._
import cluster.grpc._
import com.google.common.base.Charsets
import com.google.protobuf.ByteString
import org.apache.commons.lang3.RandomStringUtils
import org.slf4j.LoggerFactory
import services.scalable.index.grpc.{IndexContext, KVPair}
import services.scalable.index.impl.{CassandraStorage, DefaultCache, MemoryStorage}
import services.scalable.index.{Bytes, Commands, DefaultComparators, DefaultIdGenerators, DefaultPrinters, DefaultSerializers, IndexBuilder, QueryableIndex}

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.Await
import scala.concurrent.duration.Duration

object DatomDemo {

  def main(args: Array[String]): Unit = {

    val logger = LoggerFactory.getLogger(this.getClass)

    val rand = ThreadLocalRandom.current()
    import scala.concurrent.ExecutionContext.Implicits.global

    type K = EAVT
    type V = Bytes

    val EMPTY_BYTES = Array.empty[Byte]

    import services.scalable.index.DefaultComparators._
    import cluster.Comparators._

    val indexId = TestConfig.CLUSTER_INDEX_NAME

    implicit val idGenerator = DefaultIdGenerators.idGenerator
    implicit val cache = new DefaultCache(MAX_PARENT_ENTRIES = 80000)
    implicit val storage = new MemoryStorage()
    //implicit val storage = new CassandraStorage(TestConfig.session, false)

    val builder = IndexBuilder.create[K, V](Comparators.eavtOrd)
      .storage(storage)
      .serializer(Serializers.grpcEAVTBytesSerializer)
      .keyToStringConverter(Printers.eavtToStringPrinter)

    val descriptor = IndexContext()
      .withId(UUID.randomUUID.toString)
      .withNumLeafItems(TestHelper.NUM_LEAF_ENTRIES)
      .withNumMetaItems(TestHelper.NUM_META_ENTRIES)

    var data = Seq.empty[(K, V, Boolean)]
    var index = new QueryableIndex[K, V](descriptor)(builder)

    def insert(): Unit = {

      val descriptorBackup = index.descriptor

      val n = rand.nextInt(1, 1000)
      var list = Seq.empty[Tuple3[K, V, Boolean]]

      val insertDup = false//rand.nextBoolean()

      for (i <- 0 until n) {

        rand.nextBoolean() match {
          case x if x && list.length > 0 && insertDup =>

            // Inserts some duplicate
            val (k, v, _) = list(rand.nextInt(0, list.length))
            list = list :+ (k, v, false)

          case _ =>

            val name = RandomStringUtils.randomAlphanumeric(5, 10).getBytes(Charsets.UTF_8)
            val v = EMPTY_BYTES//RandomStringUtils.randomAlphanumeric(5).getBytes(Charsets.UTF_8)

            val k = EAVT()
              .withE(UUID.randomUUID.toString)
              .withA("name")
              .withV(ByteString.copyFrom(name))
              .withT(System.nanoTime())

            if (!data.exists { case (k1, _, _) => eavtOrd.equiv(k, k1) } &&
              !list.exists { case (k1, _, _) => eavtOrd.equiv(k, k1) }) {
              list = list :+ (k, v, true)
            }
        }
      }

      //logger.debug(s"${Console.GREEN_B}INSERTING ${list.map{case (k, v, _) => builder.ks(k)}}${Console.RESET}")

      val cmds = Seq(
        Commands.Insert(indexId, list)
      )

      val result = Await.result(index.execute(cmds), Duration.Inf)

      if (result.success) {
        logger.debug(s"${Console.GREEN_B}INSERTION OK: ${list.map { case (k, v, _) => builder.ks(k) }}${Console.RESET}")

        val newDescriptor = Await.result(index.save(), Duration.Inf)
        index = new QueryableIndex[K, V](newDescriptor)(builder)

        data = data ++ list

        return
      }

      logger.debug(s"${Console.RED_B}INSERTION FAIL: ${list.map { case (k, v, _) => builder.ks(k) }}${Console.RESET}")

      index = new QueryableIndex[K, V](descriptorBackup)(builder)
      result.error.get.printStackTrace()
    }

    val n = 3

    for (i <- 0 until n) {
      rand.nextInt(1, 2) match {
        case _ => insert()
      }
    }

    logger.info(Await.result(index.save(), Duration.Inf).toString)

    val dlist = data.sortBy(_._1).map { case (k, v, _) => k -> v }.toList
    val ilist = Await.result(TestHelper.all(index.inOrder()), Duration.Inf).map { case (k, v, _) => k -> v }.toList

    val dliststr = dlist.map { case (k, v) => builder.ks(k)  }
    val iliststr = ilist.map { case (k, v) => builder.ks(k) }

    logger.debug(s"${Console.GREEN_B}tdata: ${dliststr}${Console.RESET}\n")
    logger.debug(s"${Console.MAGENTA_B}idata: ${iliststr}${Console.RESET}\n")

    Await.result(storage.close(), Duration.Inf)

    assert(TestHelper.isColEqual(dlist.map{x => (x._1, x._2, "")}, ilist.map{x => (x._1, x._2, "")}))

    TestConfig.session.close()
    Await.ready(storage.close(), Duration.Inf)

  }

}
