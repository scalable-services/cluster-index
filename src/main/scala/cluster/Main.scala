package cluster

import cluster.grpc._
import com.google.common.base.Charsets
import com.google.protobuf.ByteString
import org.apache.commons.lang3.RandomStringUtils
import org.slf4j.LoggerFactory
import services.scalable.index.grpc.{IndexContext, KVPair}
import services.scalable.index.impl.{CassandraStorage, DefaultCache}
import services.scalable.index.{Bytes, Commands, DefaultComparators, DefaultIdGenerators, DefaultSerializers, IndexBuilder}
import com.google.protobuf.any.Any
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Main {

  def main(args: Array[String]): Unit = {

    val logger = LoggerFactory.getLogger(this.getClass)

    val rand = ThreadLocalRandom.current()
    import scala.concurrent.ExecutionContext.Implicits.global

    type K = String
    type V = IndexValue

    val indexId = TestConfig.CLUSTER_INDEX_NAME //UUID.randomUUID().toString

    implicit val idGenerator = DefaultIdGenerators.idGenerator
    implicit val cache = new DefaultCache(MAX_PARENT_ENTRIES = 80000)
    //implicit val storage = new MemoryStorage()
    implicit val storage = new CassandraStorage(TestConfig.session, false)

    val indexBuilder = IndexBuilder.create[K, V](DefaultComparators.ordString, DefaultSerializers.stringSerializer,
      Serializers.indexValueSerializer)
      .storage(storage)
      .serializer(Serializers.grpcStringIndexValueBytesSerializer)
      .valueToStringConverter(Printers.indexValueToString)

    val clusterMetaBuilder = IndexBuilder.create[K, KeyIndexContext](DefaultComparators.ordString,
      DefaultSerializers.stringSerializer, Serializers.keyIndexSerializer)
      .storage(storage)
      .serializer(Serializers.grpcStringKeyIndexContextSerializer)
      .valueToStringConverter(Printers.keyIndexContextToStringPrinter)

    val metaContext = Await.result(TestHelper.loadOrCreateIndex(IndexContext(
      indexId,
      TestHelper.NUM_LEAF_ENTRIES,
      TestHelper.NUM_META_ENTRIES
    ).withMaxNItems(Int.MaxValue)), Duration.Inf).get

    val COLORS = Seq("blue", "green", "red", "cyan", "yellow", "black", "white", "purple", "brown",
      "magenta", "grey")

    val ordering = indexBuilder.ord

    val EMPTY_BYTES = Array.empty[Byte]

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

    def insert(n: Int): Commands.Insert[K, V] = {

      var list = Seq.empty[Tuple3[K, V, Boolean]]

      val time = System.nanoTime()

      for (i <- 0 until n) {

        val color = COLORS(rand.nextInt(0, COLORS.length)).getBytes("UTF-8")

        val k = RandomStringUtils.randomAlphabetic(10)//.getBytes(Charsets.UTF_8)
        val v = IndexValue()
          .withValue(ByteString.copyFrom(color))
          .withTmp(time)
          .withValid(true)

        if (!list.exists { case (k1, _, _) => ordering.equiv(k, k1) }) {
          list = list :+ (k, v, false)
        }
      }

      //data = data ++ list.map{case (k, v, _) => (k, v, None)}

      Commands.Insert(indexId, list)
    }

    val insertCommands = insert(1000)

    val context = Await.result(cindex.execute(Seq(insertCommands)).flatMap(_ => cindex.save()), Duration.Inf)

    val cindexFromDisk = new ClusterIndex[K, V](
      context,
      TestHelper.MAX_ITEMS,
      TestHelper.NUM_LEAF_ENTRIES,
      TestHelper.NUM_META_ENTRIES
    )(indexBuilder, clusterMetaBuilder)

    var data = cindexFromDisk.inOrder()

    println()

    def insert2(): Seq[Commands.Command[K, V]] = {

      var list = Seq.empty[Tuple3[K, V, Boolean]]

      val n = 100//rand.nextInt(100, 1000)
      val time = System.nanoTime()

      for (i <- 0 until n) {

        val color = COLORS(rand.nextInt(0, COLORS.length)).getBytes("UTF-8")

        val k = RandomStringUtils.randomAlphabetic(10) //.getBytes(Charsets.UTF_8)
        val v = IndexValue()
          .withValue(ByteString.copyFrom(color))
          .withTmp(time)
          .withValid(true)

        if (!data.exists { case (k1, _, _) => ordering.equiv(k, k1) } &&
          !list.exists { case (k1, _, _) => ordering.equiv(k, k1) }) {
          list = list :+ (k, v, false)
        }
      }

      data = data ++ list.map{case (k, v, _) => (k, v, "")}

      println(s"insert size: ${list.length}")

      Seq(Commands.Insert(indexId, list))
    }

    def update(filterOut: Seq[K] = Seq.empty[K]): Seq[Commands.Command[K, V]] = {
      val time = System.nanoTime()
      val n = if (data.length >= 2) rand.nextInt(1, data.length) else 1

      val list = scala.util.Random.shuffle(data).slice(0, n)
        .filterNot{case (k, _, _) => filterOut.exists{ordering.equiv(_, k)}}

      if(list.isEmpty) return Seq.empty[Commands.Command[K, V]]

      var updates = Seq.empty[(K, V, Option[String])]

      list.foreach { case (k, v, lv) =>
        val filtered = COLORS.filterNot(_ == v.value.toStringUtf8)
        val color = filtered(rand.nextInt(0, filtered.length)).getBytes("UTF-8")

        updates = updates :+ (k, v.withValue(ByteString.copyFrom(color)).withTmp(time), Some(lv))
      }

      println(s"update size: ${list.length}")
      println(s"updates: ${updates.map{case (k, v, _) => indexBuilder.ks(k) -> indexBuilder.vs(v)}}")

      data = data.filterNot{case (k, _, _) => list.exists{case (k1, _, _) => ordering.equiv(k, k1)}}
      data = data ++ updates.map{case (k, v, lv) => (k, v, lv.get)}

      Seq(Commands.Update(indexId, updates))
    }

    def remove(filterOut: Seq[K] = Seq.empty[K]): Seq[Commands.Command[K, V]] = {
      val time = System.nanoTime()

      val n = if (data.length >= 2) rand.nextInt(1, data.length) else 1

      val list = scala.util.Random.shuffle(data.filter(_._2.valid)).slice(0, n)
        .filterNot{case (k, _, _) => filterOut.exists{ordering.equiv(_, k)}}
        .map { case (k, v, lv) =>
        (k, v.withTmp(time).withValid(false), Some(lv))
      }

      if(list.isEmpty) return Seq.empty[Commands.Command[K, V]]

      println(s"remove size: ${list.length}")
      println(s"removals: ${list.map{case (k, v, _) => indexBuilder.ks(k) -> indexBuilder.vs(v)}}")

      data = data.filterNot{case (k, _, _) => list.exists{case (k1, _, _) => ordering.equiv(k, k1)}}

      Seq(Commands.Update[K, V](indexId, list))
    }

    val updateList = update()
    val removeList = remove(updateList.head.asInstanceOf[Commands.Update[K, V]].list.map(_._1))
    val insertList = insert2()

    val commands = updateList ++ removeList ++ insertList

    /*Await.result(cindex.execute(commands).flatMap(_ => cindex.save()), Duration.Inf)

    val idata = cindex.inOrder().filter(_._1.valid)
    val data2 = data.sortBy(_._1)

    assert(TestHelper.isColEqual(idata, data2)(eavtOrd, DefaultComparators.bytesOrd))*/

    val listIndex = data.sortBy(_._1)

    println(s"${Console.YELLOW_B}listindex after range cmds inserted: ${Helper.saveListIndex(s"after-$indexId", listIndex, storage.session, indexBuilder.keySerializer, indexBuilder.valueSerializer)}${Console.RESET}")

    val cc = new ClusterClient[K, V](context)(clusterMetaBuilder)

    val mappedCmds = Await.result(cc.execute(commands), Duration.Inf)

    val sendCmds = mappedCmds.map { case (range, cmds) =>
      val insertions = cmds.filter(_.isInstanceOf[Commands.Insert[K, V]])
        .map(_.asInstanceOf[Commands.Insert[K, V]]).map { c =>
        InsertCommand(c.list.map { case (k, v, _) =>
          KVPair(ByteString.copyFrom(indexBuilder.keySerializer.serialize(k)), ByteString.copyFrom(Any.pack(v).toByteArray))
        }, true)
      }

      val updates = cmds.filter(_.isInstanceOf[Commands.Update[K, V]]).map(_.asInstanceOf[Commands.Update[K, V]]).map { c =>
        UpdateCommand(c.list.map { case (k, v, ls) =>
          KVPair(ByteString.copyFrom(indexBuilder.keySerializer.serialize(k)), ByteString.copyFrom(Any.pack(v).toByteArray), ls.get)
        })
      }

      RangeTask(range, insertions, updates)
    }.toSeq

    val done = Await.result(cc.sendTasks(sendCmds), Duration.Inf)
    println(s"tasks sent: ${done}...")

    /*val br = Await.result(cindexFromDisk.execute(commands).flatMap{ r =>
      println(s"\n\nBATCH RESULT: ${r.success}\n\n")
      if(!r.success) throw r.error.get
      cindexFromDisk.save()
    }, Duration.Inf)

    val brList = cindexFromDisk.inOrder().filter(_._2.valid)

    val dontMatch = listIndex.zipWithIndex.filterNot{case (k, i) => brList(i)._1 == k._1 && Comparators.indexValueOrd.equiv(brList(i)._2, k._2)}
    val matches = TestHelper.isColEqual(listIndex.toList, brList.toList)(ordering, Comparators.indexValueOrd)

    val metacIndexFromDisk = Await.result(TestHelper.all(cindexFromDisk.meta.inOrder()), Duration.Inf)*/

    cc.system.terminate()
    Await.result(storage.close().flatMap(_ => cc.system.whenTerminated), Duration.Inf)

    println()
  }

}
