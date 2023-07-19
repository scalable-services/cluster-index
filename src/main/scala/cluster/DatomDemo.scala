package cluster

import cluster.grpc._
import com.google.common.base.Charsets
import com.google.protobuf.ByteString
import org.apache.commons.lang3.RandomStringUtils
import org.slf4j.LoggerFactory
import services.scalable.index.grpc.IndexContext
import services.scalable.index.impl.{DefaultCache, MemoryStorage}
import services.scalable.index.{Bytes, Commands, DefaultComparators, DefaultIdGenerators, DefaultSerializers, IndexBuilder, QueryableIndex}

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.Await
import scala.concurrent.duration.Duration

object DatomDemo {

  def main(args: Array[String]): Unit = {

    val logger = LoggerFactory.getLogger(this.getClass)

    val rand = ThreadLocalRandom.current()
    import scala.concurrent.ExecutionContext.Implicits.global

    type K = String
    type V = IndexValue

    import cluster.Comparators._
    import services.scalable.index.DefaultComparators._

    val indexId = TestConfig.CLUSTER_INDEX_NAME

    implicit val idGenerator = DefaultIdGenerators.idGenerator
    implicit val cache = new DefaultCache(MAX_PARENT_ENTRIES = 80000)
    implicit val storage = new MemoryStorage()
    //implicit val storage = new CassandraStorage(TestConfig.session, false)

    val builder = IndexBuilder.create[K, V](DefaultComparators.ordString, DefaultSerializers.stringSerializer,
      Serializers.indexValueSerializer)
      .storage(storage)
      .serializer(Serializers.grpcStringIndexValueBytesSerializer)
      .valueToStringConverter(Printers.indexValueToString)

    val descriptor = IndexContext()
      .withId(UUID.randomUUID.toString)
      .withNumLeafItems(TestHelper.NUM_LEAF_ENTRIES)
      .withNumMetaItems(TestHelper.NUM_META_ENTRIES)

    val ordering = builder.ord

    var data = Seq.empty[(K, V, Option[String])]
    var index = new QueryableIndex[K, V](descriptor)(builder)

    val COLORS = Seq("blue", "green", "red", "cyan", "yellow", "black", "white", "purple", "brown",
      "magenta", "grey")

    def insert(): Unit = {

      val currentVersion = Some(index.ctx.id)
      val indexBackup = index

      val n = rand.nextInt(1, 1000)
      var list = Seq.empty[Tuple3[K, V, Boolean]]

      val time = System.nanoTime()

      for (i <- 0 until n) {

        val k = RandomStringUtils.randomAlphanumeric(10)

        //val color = COLORS(rand.nextInt(0, COLORS.length)).getBytes("UTF-8") //RandomStringUtils.randomAlphanumeric(5, 10).getBytes(Charsets.UTF_8)
        //val v = EMPTY_BYTES //RandomStringUtils.randomAlphanumeric(5).getBytes(Charsets.UTF_8)

        val v = IndexValue()
          .withValue(ByteString.copyFrom(COLORS(rand.nextInt(0, COLORS.length)).getBytes(Charsets.UTF_8)))
          .withTmp(time)
          .withValid(true)

        if (!data.exists { case (k1, _, _) => ordering.equiv(k, k1) } &&
          !list.exists { case (k1, _, _) => ordering.equiv(k, k1) }) {
          list = list :+ (k, v, false)
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

        data = data ++ list.map{case (k, v, _) => (k, v, currentVersion)}

        return
      }

      logger.debug(s"${Console.RED_B}INSERTION FAIL: ${list.map { case (k, v, _) => builder.ks(k) }}${Console.RESET}")

      index = indexBackup
      result.error.get.printStackTrace()
    }

    def update(): Unit = {

      val currentVersion = Some(index.ctx.id)
      val time = System.nanoTime()
      val indexBackup = index

      val n = if (data.length >= 2) rand.nextInt(1, data.length) else 1

      val list = scala.util.Random.shuffle(data).slice(0, n)
      var updates = Seq.empty[(K, V, Option[String])]

      list.foreach { case (k, v, lv) =>
        val filtered = COLORS.filterNot(_ == v.value.toStringUtf8)
        val color = filtered(rand.nextInt(0, filtered.length)).getBytes("UTF-8")

         updates = updates ++ Seq(
           //Insert the new one...
           Tuple3(k, v.withValue(ByteString.copyFrom(color)).withValid(true).withTmp(time), lv)
         )
      }

      val cmds = Seq(
        Commands.Update(indexId, updates)
      )

      val result = Await.result(index.execute(cmds), Duration.Inf)

      if (result.success) {

        logger.debug(s"${Console.MAGENTA_B}UPDATED RIGHT LAST VERSION ${list.map { case (k, _, _) => builder.ks(k) }}...${Console.RESET}")

        val newDescriptor = Await.result(index.save(), Duration.Inf)
        index = new QueryableIndex[K, V](newDescriptor)(builder)

        data = data.filterNot { case (k, _, _) => list.exists { case (k1, _, _) => ordering.equiv(k, k1) } }
        data = data ++ updates.map{case (k, v, _) => (k, v, currentVersion)}

        return
      }

      index = indexBackup
      result.error.get.printStackTrace()
      logger.debug(s"${Console.CYAN_B}UPDATED WRONG LAST VERSION ${list.map { case (k, _, _) => builder.ks(k) }}...${Console.RESET}")
    }

    def remove(): Unit = {

      val currentVersion = Some(index.ctx.id)
      val time = System.nanoTime()
      val indexBackup = index

      val n = if (data.length >= 2) rand.nextInt(1, data.length) else 1
      val list = scala.util.Random.shuffle(data.filter(_._2.valid)).slice(0, n).map { case (k, v, lv) =>
        (k, v.withValid(false).withTmp(time), lv)
      }

      val cmds = Seq(
        Commands.Update[K, V](indexId, list)
      )

      val result = Await.result(index.execute(cmds), Duration.Inf)

      if (result.success) {

        val newDescriptor = Await.result(index.save(), Duration.Inf)
        index = new QueryableIndex[K, V](newDescriptor)(builder)

        logger.debug(s"${Console.YELLOW_B}REMOVED RIGHT VERSION ${list.map { case (k, _, _) => builder.ks(k) }}...${Console.RESET}")

        data = data.filterNot { case (k, _, _) => list.exists { case (k1, _, _) => ordering.equiv(k, k1) } }
        //data = data ++ list.map{case (k, v, _) => (k, v, currentVersion)}

        return
      }

      index = indexBackup
      result.error.get.printStackTrace()
      logger.debug(s"${Console.RED_B}REMOVED WRONG VERSION ${list.map { case (k, _, _) => builder.ks(k) }}...${Console.RESET}")
    }

    val n = 10

    for (i <- 0 until n) {
      rand.nextInt(1, 4) match {
        case 2 if !data.isEmpty => update()
        case 3 if !data.isEmpty  => remove()
        case _ => insert()
      }
    }

    logger.info(Await.result(index.save(), Duration.Inf).toString)

    val dlist = data.sortBy(_._1).map { case (k, v, _) => k -> v }
      //.filter(_._2.valid)
      .toList
    val ilist = Await.result(TestHelper.all(index.inOrder()), Duration.Inf)
      .filter(_._2.valid)
      .map { case (k, v, _) => k -> v }
      .toList

    val dliststr = dlist.map { case (k, v) => builder.ks(k) -> builder.vs(v)  }
    val iliststr = ilist.map { case (k, v) => builder.ks(k) -> builder.vs(v) }

    logger.info(s"${Console.GREEN_B}tdata: ${dliststr}${Console.RESET}\n")
    logger.info(s"${Console.MAGENTA_B}idata: ${iliststr}${Console.RESET}\n")

    Await.result(storage.close(), Duration.Inf)

    assert(TestHelper.isColEqual(dlist.map{x => (x._1, x._2, "")}, ilist.map{x => (x._1, x._2, "")}))

    Await.ready(storage.close(), Duration.Inf)
  }

}
