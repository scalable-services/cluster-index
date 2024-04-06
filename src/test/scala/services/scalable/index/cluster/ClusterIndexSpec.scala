package services.scalable.index.cluster

import ch.qos.logback.classic.{Level, Logger}
import org.apache.commons.lang3.RandomStringUtils
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory
import services.scalable.index.Commands.{Insert, Remove, Update}
import services.scalable.index.cluster.grpc.KeyIndexContext
import services.scalable.index.grpc.IndexContext
import services.scalable.index.impl.{CassandraStorage, DefaultCache, MemoryStorage}
import services.scalable.index.{Commands, DefaultComparators, DefaultIdGenerators, DefaultSerializers, IndexBuilder, QueryableIndex}

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class ClusterIndexSpec extends Repeatable with Matchers {

  LoggerFactory.getLogger("services.scalable.index.Context").asInstanceOf[Logger].setLevel(Level.INFO)
  LoggerFactory.getLogger("services.scalable.index.impl.GrpcByteSerializer").asInstanceOf[Logger].setLevel(Level.INFO)

  override val times: Int = 1000

  type K = String
  type V = String

  val ordering = DefaultComparators.ordString

  "operations" should " run successfully" in {

    val indexId = TestHelper.CLUSTER_INDEX_NAME
    val rand = ThreadLocalRandom.current()

    //implicit val session = TestHelper.getSession()

    //TestHelper.truncateAll()

   // val version = TestHelper.TX_VERSION//UUID.randomUUID.toString

      val order = 16//rand.nextInt(4, 1000)
    val min = order / 2
    val max = order

    var data = Seq.empty[(K, V, String)]

    implicit val idGenerator = DefaultIdGenerators.idGenerator
    implicit val cache = new DefaultCache(MAX_PARENT_ENTRIES = 80000)
    implicit val storage = new MemoryStorage()
    //implicit val storage = new CassandraStorage(session, true)

    val rangeBuilder = IndexBuilder.create[K, V](
      global,
      ordering,
      TestHelper.MAX_RANGE_ITEMS.toInt,
      TestHelper.MAX_RANGE_ITEMS.toInt,
      TestHelper.MAX_RANGE_ITEMS,
      DefaultSerializers.stringSerializer,
      DefaultSerializers.stringSerializer)
      .storage(storage)
      .cache(cache)
      .serializer(Serializers.grpcStringStringSerializer)
      .build()

    val clusterMetaBuilder = IndexBuilder.create[K, KeyIndexContext](global,
        DefaultComparators.ordString,
        TestHelper.MAX_LEAF_ITEMS,
        TestHelper.MAX_META_ITEMS,
        -1L,
        DefaultSerializers.stringSerializer, Serializers.keyIndexSerializer)
      .storage(storage)
      .cache(cache)
      .serializer(Serializers.grpcStringKeyIndexContextSerializer)
      .valueToStringConverter(Printers.keyIndexContextToStringPrinter)
      .build()

    val metaContext = Await.result(storage.loadOrCreate(IndexContext(
      indexId,
      TestHelper.MAX_LEAF_ITEMS,
      TestHelper.MAX_META_ITEMS,
      maxNItems = -1L
    )), Duration.Inf)

    val cindex = new ClusterIndex[K, V](metaContext)(rangeBuilder, clusterMetaBuilder)

    val version = "v1"

    def insert(min: Int = 400, max: Int = 1000): Seq[Insert[K, V]] = {
      var list = Seq.empty[(K, V, Boolean)]
      val n = rand.nextInt(min, max)

      for (i <- 0 until n) {
        val k = RandomStringUtils.randomAlphanumeric(5)
        val v = RandomStringUtils.randomAlphanumeric(5)

        if (!list.exists { case (k1, _, _) => ordering.equiv(k, k1) } && !data.exists { case (k1, _, _) => ordering.equiv(k, k1) }) {
          list :+= Tuple3(k, v, false)
        }
      }

      data = data ++ list.map { case (k, v, _) => Tuple3(k, v, version) }

      Seq(Insert(indexId, list, Some(version)))
    }

    def update(): Seq[Update[K, V]] = {
      if(data.isEmpty) return Seq.empty[Update[K, V]]

      var list = scala.util.Random.shuffle(data)
      list = if(list.length > 1) list.slice(0, rand.nextInt(0, list.length)) else list

      val toUpdate = list.map { case (k, v, _) =>
        (k, RandomStringUtils.randomAlphabetic(6), Some(version))
      }

      data = data.map { case (k, v, vs) =>
        val opt = toUpdate.find(x => ordering.equiv(x._1, k))

        opt match {
          case None => (k, v, vs)
          case Some((k1, v1, _)) => (k1, v1, version)
        }
      }

      Seq(Update(indexId, toUpdate, Some(version)))
    }

    def remove(): Seq[Remove[K, V]] = {
      if(data.isEmpty) return Seq.empty[Remove[K, V]]

      var list = scala.util.Random.shuffle(data)
      list = if(list.length > 1) list.slice(0, rand.nextInt(0, list.length)) else list

      val toRemove = list.map { case (k, v, vs) =>
        (k, Some(version))
      }

      data = data.filterNot { case (k, v, _) =>
        toRemove.exists{case (k1, vs) => ordering.equiv(k, k1) }
      }

      Seq(Remove(indexId, toRemove, Some(version)))
    }

    var commands: Seq[Commands.Command[K, V]] = insert(1500, 3000)

    commands = commands ++ update() ++ remove() /*++ remove()*/

    val ctx = Await.result(cindex.execute(commands, version).flatMap(_ => cindex.save()), Duration.Inf)

    val dataSorted = data.sortBy(_._1).map{x => x._1 -> x._2}.toList
    val all = cindex.inOrder().map{case (k, v, _) => (k, v)}.toList

    assert(dataSorted == all)

    println()

   // session.close()
  }

}
