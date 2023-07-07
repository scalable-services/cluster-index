package cluster

import cluster.ClusterSerializers._
import cluster.grpc._
import com.google.common.base.Charsets
import com.google.protobuf.ByteString
import org.apache.commons.lang3.RandomStringUtils
import org.slf4j.LoggerFactory
import services.scalable.index.grpc.{IndexContext, KVPair}
import services.scalable.index.impl.{CassandraStorage, DefaultCache}
import services.scalable.index.{Bytes, Commands, Context, DefaultComparators, DefaultIdGenerators, DefaultPrinters, DefaultSerializers, IdGenerator, IndexBuilder}

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Main {

  def main(args: Array[String]): Unit = {

    val logger = LoggerFactory.getLogger(this.getClass)

    val rand = ThreadLocalRandom.current()
    import scala.concurrent.ExecutionContext.Implicits.global

    type K = Bytes
    type V = Bytes

    import services.scalable.index.DefaultComparators._

    val indexId = TestConfig.CLUSTER_INDEX_NAME //UUID.randomUUID().toString

    implicit val idGenerator = DefaultIdGenerators.idGenerator
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

    val savedMetaContext = Await.result(cindex.insert(list).flatMap(_ => cindex.save(false)), Duration.Inf)

    val elements = cindex.inOrder().toList

    logger.info(s"${Console.YELLOW_B}list data:     ${list.toList.map { case (k, v, _) => new String(k, Charsets.UTF_8) }}${Console.RESET}\n")
    logger.info(s"${Console.YELLOW_B}elements data: ${elements.map { case (k, v, _) => new String(k, Charsets.UTF_8) }}${Console.RESET}\n")

    assert(TestHelper.isColEqual(list.map{case (k, v, _) => (k, v, savedMetaContext.id)}, elements))

    val list2 = insert(0, 2222).sortBy(_._1)

    val listAll = (list ++ list2).sortBy(_._1)

    println(s"${Console.YELLOW_B}listindex before range cmds inserted: ${Helper.saveListIndex(s"before-$indexId", list.sortBy(_._1), storage.session)}${Console.RESET}")
    println(s"${Console.YELLOW_B}listindex inserted: ${Helper.saveListIndex(s"after-${indexId}", listAll, storage.session)}${Console.RESET}")

    val clusterc = new ClusterClient[K, V](savedMetaContext)(clusterMetaBuilder)

    val ranges = Await.result(clusterc.execute(Seq(Commands.Insert[K, V](indexId, list2))), Duration.Inf)

    ranges.foreach { case (id, cmds) =>
      val icmds = cmds.asInstanceOf[Seq[Commands.Insert[K, V]]]
      println(id, icmds.map{c => c.list.map{x => new String(x._1)}})
    }

    val tasks = ranges.map { case (rid, cmds) =>

      logger.info(s"${Console.YELLOW_B}range task: ${rid}${Console.RESET}")

      RangeTask(rid, cmds.map { c =>
        InsertCommand(c.asInstanceOf[Commands.Insert[K, V]].list.map { case (k, v, _) =>
          KVPair(ByteString.copyFrom(k), ByteString.copyFrom(v))
        }, true)
      }.toSeq)
    }.toSeq

    val ok = Await.result(clusterc.sendTasks(tasks), Duration.Inf)

    logger.info(s"sent commands to kafka: ${ok}")

    /*val toInsert = ranges.map { case (id, cmds) =>
      val icmds = cmds.asInstanceOf[Seq[Commands.Insert[K, V]]]
      icmds.map(_.list)
    }

    logger.info(s"${Console.CYAN_B}toInsert: ${toInsert.map(x => new String(x._1))}${Console.RESET}")
    logger.info(s"${Console.BLUE_B}list2:    ${list2.map(x => new String(x._1))}${Console.RESET}")

    assert(toInsert == list2)*/

    clusterc.system.terminate()
    Await.result(storage.close().flatMap(_ => clusterc.system.whenTerminated), Duration.Inf)

    println(s"${Console.RED_B}insert2 len: ${list2.length}${Console.RESET}")
  }

}
