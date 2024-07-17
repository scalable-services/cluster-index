package services.scalable.index.cluster

import org.apache.commons.lang3.RandomStringUtils
import org.scalatest.flatspec.AnyFlatSpec
import services.scalable.index.Commands.Insert
import services.scalable.index.grpc.IndexContext
import services.scalable.index.impl.{CassandraStorage, MemoryStorage}
import services.scalable.index.{Commands, DefaultComparators, DefaultSerializers, IndexBuilder}

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import scala.collection.concurrent.TrieMap
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

class RandomOpsClusterSpec extends AnyFlatSpec with Repeatable {

  override val times: Int = 1

  type K = String
  type V = String

  "it" should "run successfully" in {

    val rand = ThreadLocalRandom.current()
    val MAX_ITEMS = rand.nextInt()
    val NUM_LEAF_ENTRIES = rand.nextInt(4, 64)
    val NUM_META_ENTRIES = rand.nextInt(4, 64)

    //val session = TestHelper.getSession()
    val storage = /*new CassandraStorage(session, true)*/new MemoryStorage()

    val indexContext = IndexContext()
      .withId(UUID.randomUUID().toString)
      .withLevels(0)
      .withNumElements(0L)
      .withMaxNItems(MAX_ITEMS)
      .withNumLeafItems(MAX_ITEMS)
      .withNumMetaItems(MAX_ITEMS)
      .withLastChangeVersion(UUID.randomUUID().toString)

    val rangeBuilder = IndexBuilder.create[K, V](global, DefaultComparators.ordString,
        indexContext.numLeafItems, indexContext.numMetaItems, indexContext.maxNItems,
        DefaultSerializers.stringSerializer, DefaultSerializers.stringSerializer)
      .storage(storage)
      .serializer(ClusterSerializers.grpcStringStringSerializer)
      .keyToStringConverter(k => k)
      .build()

    val version = "v1"
    val clusterIndexId = "ci2"

    val clusterIndexDescriptor = Await.result(
      storage.loadOrCreate(IndexContext()
        .withId(clusterIndexId)
        .withLevels(0)
        .withNumElements(0L)
        .withMaxNItems(Long.MaxValue)
        .withNumLeafItems(NUM_LEAF_ENTRIES)
        .withNumMetaItems(NUM_META_ENTRIES)
        .withLastChangeVersion(UUID.randomUUID().toString)),
      Duration.Inf
    )

    var data = Seq.empty[(K, V)]

    def insert(): Unit = {
      val n = rand.nextInt(100, 1000)

      val list = (0 until n).map{ _ =>
        val k = RandomStringUtils.randomAlphabetic(10).toLowerCase()
        val v = k

        (k, v, true)
      }.filterNot{case (k, _, _) => data.exists{case (k1, _) => rangeBuilder.ord.equiv(k, k1)}}

      if(list.isEmpty) {
        println("no unique data to insert!")
        return
      }

      val ctx = Await.result(storage.loadIndex(clusterIndexId), Duration.Inf).get
      val cindex = new ClusterIndex[K, V](ctx)(rangeBuilder)

      val r1 = Await.result(cindex.insert(list, version), Duration.Inf)

      assert(r1.success)

      val r2 = Await.result(cindex.save(), Duration.Inf)

      data = data ++ list.map{case (k, v, _) => k -> v}

      println(s"${Console.GREEN_B}INSERTING ${list.length}${Console.RESET}")

      ///if(list.isEmpty) return Seq.empty[Commands.Insert[K, V]]

      //Seq(Commands.Insert(clusterIndexId, list, Some(version)))
    }

    def remove(): Unit = {

      if(data.isEmpty) {
        println("no data to remove! Index is empty already!")
        return
      }

      val keys = data.map(_._1)
      val toRemoveRandom = (if(keys.length > 1) scala.util.Random.shuffle(keys).slice(0, rand.nextInt(1, keys.length))
        else keys).map { _ -> Some(version)}

      val ctx = Await.result(storage.loadIndex(clusterIndexId), Duration.Inf).get
      val cindex = new ClusterIndex[K, V](ctx)(rangeBuilder)

      val r1 = Await.result(cindex.remove(toRemoveRandom, version), Duration.Inf)

      assert(r1.success)

      val r2 = Await.result(cindex.save(), Duration.Inf)

      data = data.filterNot{case (k, v) => toRemoveRandom.exists{case (k1, _) => rangeBuilder.ord.equiv(k, k1)}}

      println(s"${Console.RED_B}REMOVING ${toRemoveRandom.length}${Console.RESET}")
    }

    val n = 100

    for(i<-0 until n){
      rand.nextInt(1, 3)  match {
        case 1 => remove()
        case _ => insert()
      }
    }

    val ctx = Await.result(storage.loadIndex(clusterIndexId), Duration.Inf).get
    val cindex = new ClusterIndex[K, V](ctx)(rangeBuilder)

    val dataOrdered = data.iterator.toSeq.sortBy(_._1).map(_._1).toList
    val indexOrdered = cindex.inOrder().map(_._1).toList

    println(s"dataordered: ${dataOrdered.map(k => rangeBuilder.ks(k))}")
    println(s"dataordered: ${indexOrdered.map(k => rangeBuilder.ks(k))}")

    assert(indexOrdered == dataOrdered)

    storage.close()
    //session.close()
  }
}
