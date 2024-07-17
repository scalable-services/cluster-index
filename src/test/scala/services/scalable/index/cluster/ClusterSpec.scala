package services.scalable.index.cluster

import org.apache.commons.lang3.RandomStringUtils
import org.scalatest.flatspec.AnyFlatSpec
import services.scalable.index.grpc.IndexContext
import services.scalable.index.impl.{CassandraStorage, MemoryStorage}
import services.scalable.index.{DefaultComparators, DefaultSerializers, IndexBuilder}

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

class ClusterSpec extends AnyFlatSpec with Repeatable {

  override val times: Int = 1

  type K = String
  type V = String

  val MAX_ITEMS = 256
  val NUM_LEAF_ENTRIES = 8//rand.nextInt(4, 64)
  val NUM_META_ENTRIES = 8//rand.nextInt(4, 64)
  val rand = ThreadLocalRandom.current()

  "it" should "run successfully" in {

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

    val clusterIndexDescriptor = Await.result(
      storage.loadOrCreate(IndexContext()
        .withId("i1")
        .withLevels(0)
        .withNumElements(0L)
        .withMaxNItems(Long.MaxValue)
        .withNumLeafItems(NUM_LEAF_ENTRIES)
        .withNumMetaItems(NUM_META_ENTRIES)
        .withLastChangeVersion(UUID.randomUUID().toString)),
      Duration.Inf
    )

    var data = Seq.empty[(K, V)]

    val n = rand.nextInt(3000, 5000)

    for(i<-1 to n){
      val k: K = RandomStringUtils.randomAlphabetic(6).toLowerCase()

      if(!data.exists{case (k1, _) => rangeBuilder.ord.equiv(k, k1)}){
        data = data :+ (k, k)
      }
    }

    val clusterIndex = new ClusterIndex[K, V](clusterIndexDescriptor)(rangeBuilder)

    val result = Await.result(clusterIndex.insert(data.map(x => (x._1, x._2, true)), version), Duration.Inf)

    var indexOrdered = clusterIndex.inOrder().map(_._1)
    var dataOrdered = data.sortBy(_._1).map(_._1)

    if(indexOrdered != dataOrdered){
      assert(false)
    }

    val toRemove = data.slice(0, /*rand.nextInt(1500, n)*/n).map{case (k, v) => (k, Some(version))}

    val ctx2 = Await.result(clusterIndex.save(), Duration.Inf)

    val c2 = new ClusterIndex[K, V](ctx2)(rangeBuilder)

    val result1 = Await.result(c2.remove(toRemove, version), Duration.Inf)

    data = data.filterNot{case (k, _) => toRemove.exists{case (k1, _) => rangeBuilder.ord.equiv(k, k1)}}

    indexOrdered = c2.inOrder().map(_._1)
    dataOrdered = data.sortBy(_._1).map(_._1)

    if(indexOrdered != dataOrdered){
      assert(false)
    }

    storage.close()
    //session.close()
  }
}
