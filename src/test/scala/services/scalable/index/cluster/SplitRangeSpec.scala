package services.scalable.index.cluster

import io.netty.util.internal.ThreadLocalRandom
import org.apache.commons.lang3.RandomStringUtils
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory
import services.scalable.index.Commands.Insert
import services.scalable.index.grpc.IndexContext
import services.scalable.index.impl.{DefaultCache, MemoryStorage}
import services.scalable.index.{DefaultComparators, DefaultSerializers, IndexBuilder}

import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class SplitRangeSpec extends Repeatable with Matchers {

  val logger = LoggerFactory.getLogger(this.getClass)

  override val times: Int = 1000

  "operations" should " run successfully" in {

    val rand = ThreadLocalRandom.current()
    import scala.concurrent.ExecutionContext.Implicits.global

    type K = String
    type V = String

    val indexId1 = "index1"

    implicit val ord = DefaultComparators.ordString
    val version = "v1"

    // val session = TestHelper.createCassandraSession()
    val storage = /*new CassandraStorage(session, true)*/ new MemoryStorage()

    val MAX_ITEMS = rand.nextInt(100, 1000)

    logger.info(s"max_items: ${MAX_ITEMS}")

    val indexContext1 = IndexContext()
      .withId(indexId1)
      .withLevels(0)
      .withNumElements(0)
      .withMaxNItems(MAX_ITEMS)
      .withLastChangeVersion(UUID.randomUUID().toString)
      .withNumLeafItems(MAX_ITEMS)
      .withNumMetaItems(MAX_ITEMS)

    val cache = new DefaultCache()

    val rangeBuilder = IndexBuilder.create[K, V](global, DefaultComparators.ordString,
        indexContext1.numLeafItems, indexContext1.numMetaItems, indexContext1.maxNItems,
        DefaultSerializers.stringSerializer, DefaultSerializers.stringSerializer)
      .storage(storage)
      .cache(cache)
      .serializer(ClusterSerializers.grpcStringStringSerializer)
      .keyToStringConverter(k => k)
      .build()

    Await.result(storage.loadOrCreate(indexContext1), Duration.Inf)

    val range1 = new LeafRange[K, V](indexContext1)(rangeBuilder)

    def insert(indexId: String, n: Int): Insert[K, V] = {
      var list = Seq.empty[(K, V, Boolean)]

      for(i<-0 until n){
        val k = RandomStringUtils.randomAlphanumeric(6)
        val v = k

        if(!list.exists{case (k1, _, _) => ord.equiv(k1, k)}){
          list = list :+ (k, v, false)
        }
      }

      Insert(indexId, list, Some(version))
    }

    val cmd = insert(indexId1, MAX_ITEMS)
    val all = cmd.list.sortBy(_._1)

    val r1 = Await.result(range1.execute(Seq(Insert(indexId1, all, Some(version))), version),
      Duration.Inf)

    val range1LenBeforeSplitting = cmd.list.length

    assert(r1.success)

    val range2 = Await.result(range1.split(), Duration.Inf).asInstanceOf[LeafRange[K, V]]

    assert(range1.ctx.num_elements == range1LenBeforeSplitting/2)
    assert(range2.ctx.num_elements == range1LenBeforeSplitting - range1.ctx.num_elements)

    val range1Data = range1.inOrderSync().map(x => (x._1, x._2)).toList
    val range2Data = range2.inOrderSync().map(x => (x._1, x._2)).toList
    val allData = all.sortBy(_._1).map{x => (x._1, x._2)}.toList

    assert((range1Data ++ range2Data) == allData)

    val range3 = Await.result(range1.merge(range2, version), Duration.Inf).asInstanceOf[LeafRange[K, V]]

    assert(range3.ctx.num_elements == all.length)

    val range3Data = range3.inOrderSync().map(x => (x._1, x._2)).toList

    assert(range3Data == allData)
  }

}
