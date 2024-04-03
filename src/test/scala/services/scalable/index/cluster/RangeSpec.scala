package services.scalable.index.cluster

import org.apache.commons.lang3.RandomStringUtils
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory
import services.scalable.index.Commands.Insert
import services.scalable.index.grpc.IndexContext
import services.scalable.index.impl.{CassandraStorage, DefaultCache, MemoryStorage}
import services.scalable.index.{Commands, DefaultComparators, DefaultIdGenerators, DefaultSerializers, IndexBuilder}

import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

class RangeSpec extends Repeatable with Matchers {

  val logger = LoggerFactory.getLogger(this.getClass)

  //LoggerFactory.getLogger("services.scalable.index.Context").asInstanceOf[Logger].setLevel(Level.INFO)
  //LoggerFactory.getLogger("services.scalable.index.impl.GrpcByteSerializer").asInstanceOf[Logger].setLevel(Level.INFO)

  override val times: Int = 1000

  type K = String
  type V = String

  val ordering = DefaultComparators.ordString

  "operations" should " run successfully" in {

    val indexId = TestHelper.CLUSTER_INDEX_NAME
    val rand = ThreadLocalRandom.current()

    val version = TestHelper.TX_VERSION//UUID.randomUUID.toString

    var data = Seq.empty[(K, V, String)]

    val session = TestHelper.getSession()

    implicit val idGenerator = DefaultIdGenerators.idGenerator
    implicit val cache = new DefaultCache(MAX_PARENT_ENTRIES = 80000)
    //implicit val storage = new MemoryStorage()
    implicit val storage = new CassandraStorage(session, true)

    val MAX_N_ITEMS = 10000
    
    val rangeBuilder = IndexBuilder.create[K, V](
      global,
      ordering,
        MAX_N_ITEMS,
        MAX_N_ITEMS,
        MAX_N_ITEMS = MAX_N_ITEMS,
      DefaultSerializers.stringSerializer,
      DefaultSerializers.stringSerializer)
      .storage(storage)
      .cache(cache)
      .serializer(Serializers.grpcStringStringSerializer)
      .build()

    val rangeId = "range-1"

    val rangeContext = Await.result(storage.loadOrCreate(IndexContext(
      rangeId,
      MAX_N_ITEMS,
      MAX_N_ITEMS,
      maxNItems = MAX_N_ITEMS
    )), Duration.Inf)

    val range = new RangeIndex[K, V](rangeContext)(rangeBuilder)

    println()

    def insert(): Boolean = {

      logger.info(s"inserting...")

      var list = Seq.empty[(K, V, Boolean)]
      val n = rand.nextInt(10, 1000)

      for (i <- 0 until n) {
        val k = RandomStringUtils.randomAlphanumeric(5)
        val v = RandomStringUtils.randomAlphanumeric(5)

        if (!list.exists { case (k1, _, _) => ordering.equiv(k, k1) } && !data.exists { case (k1, _, _) => ordering.equiv(k, k1) }) {
          list :+= Tuple3(k, v, false)
        }
      }

      if(data.length + list.length > MAX_N_ITEMS) return true

      data = data ++ list.map { case (k, v, _) => Tuple3(k, v, version) }

      val commands = Seq(Insert(indexId, list, Some(version)))

      val result = Await.result(range.execute(commands, TestHelper.TX_VERSION)
        /*.flatMap(r => range.save().map(_ => r))*/, Duration.Inf)

      assert(result.success)

      result.success
    }

    def update(): Boolean = {
      if(data.isEmpty) return true

      logger.info(s"updating...")

      val shuffled = scala.util.Random.shuffle(data)
      val toUpdate = shuffled.slice(0, rand.nextInt(0, shuffled.length)).map { case (k, v, vs) =>
        (k, RandomStringUtils.randomAlphanumeric(10), vs)
      }

      val commands = Seq(Commands.Update[K, V](indexId, toUpdate.map{case (k, v, vs) => (k, v, Some(vs))}))

      val result = Await.result(range.execute(commands, TestHelper.TX_VERSION)
        /*.flatMap(r => range.save().map(_ => r))*/, Duration.Inf)

      assert(result.success)

      data = data.map { case (k, v, vs) =>
        val opt = toUpdate.find{case (k1, _, _) => ordering.equiv(k1, k) }

        if(opt.isDefined){
          (k, opt.get._2, opt.get._3)
        } else {
          (k, v, vs)
        }
      }

      result.success
    }

    def remove(): Boolean = {
      if(data.isEmpty) return true

      logger.info(s"removing...")

      val shuffled = scala.util.Random.shuffle(data)
      val toRemove = shuffled.slice(0, rand.nextInt(0, shuffled.length)).map { case (k, _, vs) =>
        (k, Some(vs))
      }

      val commands = Seq(Commands.Remove[K, V](indexId, toRemove))

      val result = Await.result(range.execute(commands, TestHelper.TX_VERSION)
        /*.flatMap(r => range.save().map(_ => r))*/, Duration.Inf)

      assert(result.success)

      data = data.filterNot { case (k, _, _) =>
        toRemove.exists{case (k1, _) => ordering.equiv(k1, k)}
      }

      result.success
    }

    for(i<-0 until 10){
      rand.nextInt(1, 3) match {
        case 1 => insert()
        case 2 => update()
        case 3 => remove()
      }
    }

    val dataSorted = data.sortBy(_._1).map{case (k, v, _) => (k, v)}
    val rangeData = Await.result(range.inOrder(), Duration.Inf).map{case (k, v, _) => (k, v)}

    assert(rangeData == dataSorted)

    Await.result(range.save(), Duration.Inf)

    val ctxFromDisk = Await.result(storage.loadIndex(rangeId), Duration.Inf).get
    val rangeFromStorage = new RangeIndex[K, V](ctxFromDisk)(rangeBuilder)

    val rangeDataFromStorage = Await.result(rangeFromStorage.inOrder(), Duration.Inf).map{case (k, v, vs) => (k, v)}

    assert(dataSorted == rangeDataFromStorage)

    if(rangeFromStorage.hasEnough()){
      val right = Await.result(rangeFromStorage.split(), Duration.Inf)

      val leftHalf = Await.result(rangeFromStorage.inOrder(), Duration.Inf).map{case (k, v, vs) => (k, v)}
      val rightHalf = Await.result(right.inOrder(), Duration.Inf).map{case (k, v, vs) => (k, v)}

      val mergeOfHalfs = leftHalf ++ rightHalf

      assert(mergeOfHalfs == rangeDataFromStorage)

      val merged = Await.result(rangeFromStorage.merge(right, version), Duration.Inf)

      val mergedCtx = Await.result(merged.save(), Duration.Inf)

      val mergedFromStorageCtx = Await.result(storage.loadIndex(mergedCtx.id), Duration.Inf).get
      val mergedFromStorage = new RangeIndex[K, V](mergedFromStorageCtx)(rangeBuilder)

      val mergedFromStorageData = Await.result(mergedFromStorage.inOrder(), Duration.Inf)
        .map{case (k, v, vs) => (k, v)}
      val mergeRangesData = Await.result(merged.inOrder(), Duration.Inf).map{case (k, v, vs) => (k, v)}

      assert(mergeOfHalfs == mergedFromStorageData)
      assert(mergeOfHalfs == mergeRangesData)
    }

    println("ok")

    session.close()

    //println()
  }

}
