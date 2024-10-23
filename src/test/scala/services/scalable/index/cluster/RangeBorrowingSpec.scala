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

class RangeBorrowingSpec extends Repeatable with Matchers {

  val logger = LoggerFactory.getLogger(this.getClass)

  override val times: Int = 1000

  "operations" should " run successfully" in {

    val rand = ThreadLocalRandom.current()
    import scala.concurrent.ExecutionContext.Implicits.global

    type K = String
    type V = String

    val indexId1 = "index1"
    val indexId2 = "index2"

    implicit val ord = DefaultComparators.ordString
    val version = "v1"

    // val session = TestHelper.createCassandraSession()
    val storage = /*new CassandraStorage(session, true)*/ new MemoryStorage()

    val MAX_ITEMS = rand.nextInt(6, 1000)

    logger.info(s"max_items: ${MAX_ITEMS}")

    val indexContext1 = IndexContext()
      .withId(indexId1)
      .withLevels(0)
      .withNumElements(0)
      .withMaxNItems(MAX_ITEMS)
      .withLastChangeVersion(UUID.randomUUID().toString)
      .withNumLeafItems(MAX_ITEMS)
      .withNumMetaItems(MAX_ITEMS)

    val indexContext2 = IndexContext()
      .withId(indexId2)
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
    Await.result(storage.loadOrCreate(indexContext2), Duration.Inf)

    val range1 = new LeafRange[K, V](indexContext1)(rangeBuilder)
    val range2 = new LeafRange[K, V](indexContext2)(rangeBuilder)

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

    var randLen = rand.nextInt(1, MAX_ITEMS - 1)
    val otherlen = MAX_ITEMS - randLen
    randLen = if(randLen >= rangeBuilder.MAX_N_ITEMS/2 && otherlen >= rangeBuilder.MAX_N_ITEMS/2)
      randLen + (if(rand.nextBoolean()) -2 else 2) else randLen

    val r1 = Await.result(range1.execute(Seq(Insert(indexId1, all.slice(0, randLen), Some(version))), version),
      Duration.Inf)
    val r2 = Await.result(range2.execute(Seq(Insert(indexId2, all.slice(randLen, all.length),
      Some(version))), version), Duration.Inf)

    assert(r1.success)
    assert(r2.success)

    if(!(!Await.result(range1.hasMinimum(), Duration.Inf) || !Await.result(range2.hasMinimum(), Duration.Inf))){
      assert(false)
    }

    if(Await.result(range1.hasEnough(), Duration.Inf)){
      Await.result(range1.borrow(range2), Duration.Inf)
    } else {
      Await.result(range2.borrow(range1), Duration.Inf)
    }

    val range1Data = range1.inOrderSync().map(x => (x._1, x._2))
    val range2Data = range2.inOrderSync().map(x => (x._1, x._2))

    assert(Await.result(range1.hasMinimum(), Duration.Inf))
    assert(Await.result(range2.hasMinimum(), Duration.Inf))

    val indexesData = range1Data ++ range2Data
    val allData = all.map{x => (x._1, x._2)}

    if(indexesData != allData){
      assert(false)
    } else {
      assert(true)
    }

  }

}
