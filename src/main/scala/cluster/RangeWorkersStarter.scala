package cluster

import cluster.grpc.{IndexValue, KeyIndexContext}
import services.scalable.index.impl.{CassandraStorage, DefaultCache}
import services.scalable.index.{DefaultComparators, DefaultIdGenerators, DefaultPrinters, DefaultSerializers, IndexBuilder}

import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object RangeWorkersStarter {

  def main(args: Array[String]): Unit = {

    val systems = (0 until 1).map { i =>

      val cache = new DefaultCache(MAX_PARENT_ENTRIES = 80000)
      //implicit val storage = new MemoryStorage(NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
      implicit val storage = new CassandraStorage(TestConfig.session, false)

      val indexBuilder = IndexBuilder.create[String, IndexValue](DefaultComparators.ordString,
        DefaultSerializers.stringSerializer, Serializers.indexValueSerializer)
        .storage(storage)
        .cache(cache)
        .serializer(Serializers.grpcStringIndexValueBytesSerializer)
        .valueToStringConverter(Printers.indexValueToString)

      val clusterMetaBuilder = IndexBuilder.create[String, KeyIndexContext](DefaultComparators.ordString,
        DefaultSerializers.stringSerializer, Serializers.keyIndexSerializer)
        .storage(storage)
        .cache(cache)
        .serializer(Serializers.grpcStringKeyIndexContextSerializer)
        .valueToStringConverter(Printers.keyIndexContextToStringPrinter)

      new RangeTaskWorker(s"range-worker-$i")(indexBuilder, clusterMetaBuilder).vertx
    }

    //Await.result(Future.sequence(systems.map(_.close().toCompletionStage.toScala)), Duration.Inf)
  }

}
