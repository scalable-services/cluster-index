/*package services.scalable.index.cluster.test

import services.scalable.index.DefaultComparators._
import services.scalable.index.cluster.grpc.{BankAccount, KeyIndexContext}
import services.scalable.index.cluster.{ClusterIndex, Printers, Serializers}
import services.scalable.index.grpc.IndexContext
import services.scalable.index.impl.{CassandraStorage, DefaultCache, MemoryStorage}
import services.scalable.index.{Commands, DefaultComparators, DefaultIdGenerators, DefaultSerializers, IndexBuilder}

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import scala.collection.concurrent.TrieMap
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

object PopulateBank {

  val rand = ThreadLocalRandom.current()

  type K = String
  type V = BankAccount

  //val session = TestConfig.getSession()

  def main(args: Array[String]): Unit = {

    val tx = UUID.randomUUID.toString
    var commands = Seq.empty[Commands.Command[K, V]]
    val accounts = TrieMap.empty[String, BankAccount]
    val n = 1000

    for(i<-0 until n){
      val balance = rand.nextLong(0, 1000L)
      val version = UUID.randomUUID().toString
      val a = BankAccount(UUID.randomUUID().toString, balance, version)

      accounts.put(a.id, a)

      commands = commands :+ Commands.Insert(
        TestConfig.CLUSTER_INDEX,
        Seq((a.id, a, false)),
        Some(tx)
      )
    }

    //implicit val idGenerator = DefaultIdGenerators.idGenerator
    implicit val cache = new DefaultCache(MAX_PARENT_ENTRIES = 80000)
    implicit val storage = new MemoryStorage()
    //implicit val storage = new CassandraStorage(session, true)

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

    val metaContext = Await.result(storage.loadOrCreate(IndexContext(
      TestConfig.CLUSTER_INDEX,
      TestConfig.MAX_LEAF_ITEMS,
      TestConfig.MAX_META_ITEMS,
      maxNItems = -1L
    )), Duration.Inf)

    val cindex = new ClusterIndex[K, V](metaContext)(rangeBuilder, clusterMetaBuilder)

    val ctx = Await.result(cindex.execute(commands, tx).flatMap(_ => cindex.save()), Duration.Inf)

    //session.close()
    println()
  }

}
*/