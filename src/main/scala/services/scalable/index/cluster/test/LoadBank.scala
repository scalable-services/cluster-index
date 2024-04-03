/*package services.scalable.index.cluster.test

import services.scalable.index.DefaultComparators._
import services.scalable.index.cluster.grpc.{BankAccount, KeyIndexContext}
import services.scalable.index.cluster.{ClusterIndex, Printers, Serializers}
import services.scalable.index.grpc.IndexContext
import services.scalable.index.impl.{CassandraStorage, DefaultCache}
import services.scalable.index.{Commands, DefaultComparators, DefaultIdGenerators, DefaultSerializers, IndexBuilder}

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import scala.collection.concurrent.TrieMap
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

object LoadBank {

  val rand = ThreadLocalRandom.current()

  type K = String
  type V = BankAccount

  val session = TestConfig.getSession()

  def main(args: Array[String]): Unit = {

    implicit val idGenerator = DefaultIdGenerators.idGenerator
    implicit val cache = new DefaultCache(MAX_PARENT_ENTRIES = 80000)
    //implicit val storage = new MemoryStorage()
    implicit val storage = new CassandraStorage(session, false)

    val rangeBuilder = IndexBuilder.create[K, V](
        global,
        ordString,
        TestConfig.MAX_LEAF_ITEMS,
        TestConfig.MAX_META_ITEMS,
        TestConfig.MAX_RANGE_ITEMS,
        DefaultSerializers.stringSerializer,
        Serializers.bankAccountSerializer)
      .storage(storage)
      .cache(cache)
      .serializer(Serializers.grpcStringBankAccountSerializer)
      .build()

    val clusterMetaBuilder = IndexBuilder.create[K, KeyIndexContext](global,
        DefaultComparators.ordString,
        TestConfig.MAX_LEAF_ITEMS,
        TestConfig.MAX_META_ITEMS,
        -1L,
        DefaultSerializers.stringSerializer, Serializers.keyIndexSerializer)
      .storage(storage)
      .cache(cache)
      .serializer(Serializers.grpcStringKeyIndexContextSerializer)
      .valueToStringConverter(Printers.keyIndexContextToStringPrinter)
      .build()

    val metaContext = Await.result(storage.loadIndex(TestConfig.CLUSTER_INDEX), Duration.Inf).get

    val cindex = new ClusterIndex[K, V](metaContext)(rangeBuilder, clusterMetaBuilder)

    val inOrder = cindex.inOrder()

    session.close()

    println()
  }

}*/
