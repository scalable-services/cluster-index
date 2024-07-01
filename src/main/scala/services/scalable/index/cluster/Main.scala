package services.scalable.index.cluster

import com.google.common.base.Charsets
import io.netty.util.internal.ThreadLocalRandom
import org.apache.commons.lang3.RandomStringUtils
import org.slf4j.LoggerFactory
import services.scalable.index.{Bytes, DefaultComparators, DefaultPrinters, DefaultSerializers, IndexBuilder}
import services.scalable.index.grpc.IndexContext
import services.scalable.index.impl.{CassandraStorage, MemoryStorage}

import java.util.UUID
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

    val NUM_LEAF_ENTRIES = 16//rand.nextInt(4, 64)
    val NUM_META_ENTRIES = 16//rand.nextInt(4, 64)

    val indexId = UUID.randomUUID().toString

    val session = TestHelper.createCassandraSession()
    val storage = new CassandraStorage(session, true)//new MemoryStorage()

    var data = Seq.empty[(K, V, Option[String])]

    val MAX_ITEMS = 512

    val indexContext = IndexContext()
      .withId(UUID.randomUUID().toString)
      .withLevels(0)
      .withNumElements(0L)
      .withMaxNItems(MAX_ITEMS)
      .withNumLeafItems(MAX_ITEMS)
      .withNumMetaItems(MAX_ITEMS)
      .withLastChangeVersion(UUID.randomUUID().toString)

    val rangeBuilder = IndexBuilder.create[K, V](global, DefaultComparators.bytesOrd,
        indexContext.numLeafItems, indexContext.numMetaItems, indexContext.maxNItems,
        DefaultSerializers.bytesSerializer, DefaultSerializers.bytesSerializer)
      .storage(storage)
      .serializer(DefaultSerializers.grpcBytesBytesSerializer)
      .keyToStringConverter(DefaultPrinters.byteArrayToStringPrinter)
      .build()

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

    val clusterIndex = new ClusterIndex[K, V](clusterIndexDescriptor)(rangeBuilder)

    val version = "v1"

    for(i<-0 until 5000){
      val k = RandomStringUtils.randomAlphabetic(10).getBytes(Charsets.UTF_8)

      if(!data.exists{case (k1, _, _) => rangeBuilder.ord.equiv(k, k1)}){
        data = data :+ (k, k, Some(version))
      }
    }

    val result = Await.result(clusterIndex.insert(data.map(x => (x._1, x._2, true)), version), Duration.Inf)

    val ctx = Await.result(clusterIndex.save(), Duration.Inf)

    val ci2 = new ClusterIndex[K, V](ctx)(rangeBuilder)

    val dordered = data.sortBy(_._1).map(x => new String(x._1))
    val ordered = ci2.inOrder().map(x => new String(x._1))

    assert(dordered == ordered)

    println("finished")

    Await.result(storage.close(), Duration.Inf)

  }

}
