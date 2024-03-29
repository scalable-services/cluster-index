package cluster

import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.scaladsl.{Committer, Consumer}
import akka.kafka.{CommitDelivery, CommitterSettings, ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.Sink
import cluster.grpc.{KeyIndexContext, MetaTask}
import com.google.protobuf.any.Any
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.slf4j.LoggerFactory
import services.scalable.index.DefaultComparators._
import services.scalable.index.grpc.IndexContext
import services.scalable.index.impl.{CassandraStorage, DefaultCache}
import services.scalable.index.{AsyncIndexIterator, Bytes, Commands, Context, DefaultComparators, DefaultIdGenerators, DefaultPrinters, IdGenerator, IndexBuilder, QueryableIndex, Serializer, Tuple}
import cluster.ClusterSerializers._

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class MetaTaskWorker {

  val logger = LoggerFactory.getLogger(this.getClass)

  implicit val system = ActorSystem.create()
  implicit val ec = system.dispatcher

  val consumerSettings = ConsumerSettings[String, Array[Byte]](system, new StringDeserializer, new ByteArrayDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId(s"meta-task-workers")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    .withClientId(s"meta-task-worker")
    .withPollInterval(java.time.Duration.ofMillis(10L))
    .withStopTimeout(java.time.Duration.ofHours(1))
    //.withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")
  //.withStopTimeout(java.time.Duration.ofSeconds(1000L))

  val committerSettings = CommitterSettings(system).withDelivery(CommitDelivery.waitForAck)

  type K = Bytes
  type V = IndexContext

  val NUM_LEAF_ENTRIES = 8
  val NUM_META_ENTRIES = 8

  implicit val idGenerator = DefaultIdGenerators.idGenerator

  implicit val cache = new DefaultCache(MAX_PARENT_ENTRIES = 80000)
  //implicit val storage = new MemoryStorage(NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
  implicit val storage = new CassandraStorage(TestConfig.session, false)

  implicit val metaIndexSerializer = new Serializer[IndexContext] {
    override def serialize(t: IndexContext): Bytes = Any.pack(t).toByteArray

    override def deserialize(b: Bytes): IndexContext = Any.parseFrom(b).unpack(IndexContext)
  }

  val clusterMetaBuilder = IndexBuilder.create[K, KeyIndexContext](DefaultComparators.bytesOrd)
    .storage(storage)
    .serializer(grpcByteArrayKeyIndexContextSerializer)
    .keyToStringConverter(DefaultPrinters.byteArrayToStringPrinter)

  def all[K, V](it: AsyncIndexIterator[Seq[Tuple[K, V]]])(implicit ec: ExecutionContext): Future[Seq[Tuple[K, V]]] = {
    it.hasNext().flatMap {
      case true => it.next().flatMap { list =>
        all(it).map {
          list ++ _
        }
      }
      case false => Future.successful(Seq.empty[Tuple[K, V]])
    }
  }

  def handler(msg: CommittableMessage[String, Array[Byte]]): Future[Boolean] = {
    val rec = msg.record
    val task = Any.parseFrom(rec.value()).unpack(MetaTask)

    logger.info(s"\n${Console.MAGENTA_B}meta task: ${task.id}${Console.RESET}\n")

    val removeList = task.removeRanges.map { k =>
      Tuple2(k.toByteArray, None)
    }

    val insertList = task.insertRanges.map { tuple =>
      Tuple3(tuple.key.toByteArray, KeyIndexContext(tuple.key, tuple.ctxId), true)
    }

    val ctx = Await.result(storage.loadIndex(TestConfig.CLUSTER_INDEX_NAME), Duration.Inf).get
    val meta = new QueryableIndex[K, KeyIndexContext](ctx)(clusterMetaBuilder)

    val metaList = Await.result(TestHelper.all(meta.inOrder()), Duration.Inf).map{case (k, ctx, _) => new String(k)}

    println(s"${Console.MAGENTA_B}meta list: ${metaList}${Console.RESET}")

    val oldMetaList = Await.result(TestHelper.all(meta.inOrder()), Duration.Inf)

    println(s"${Console.YELLOW_B}old meta list: ${oldMetaList.map{x => new String(x._1)}}${Console.RESET}")

    for {
      _ <- if(!removeList.isEmpty) meta.execute(Seq(Commands.Remove(ctx.id, removeList))) else
        Future.successful(true)
      _ <- meta.execute(Seq(Commands.Insert(ctx.id, insertList)))
      _ <- meta.save()
    } yield {
      logger.info(s"\n${Console.GREEN_B}FINISHED!${Console.RESET}\n")
      true
    }
  }

  val control = {
    Consumer
      .committableSource(consumerSettings, Subscriptions.topics(TestConfig.META_INDEX_TOPIC))
      .mapAsync(1) { msg =>
        handler(msg).map(_ => msg.committableOffset)
      }
      /*.map { msg =>
        Await.result(handler(msg), Duration.Inf)
        msg.committableOffset
      }*/
      //.log("debugging")
      .via(Committer.flow(committerSettings.withMaxBatch(1)))
      /*.toMat(Sink.ignore)(DrainingControl.apply)
      .run().streamCompletion*/
      .runWith(Sink.ignore)
      .recover {
        case e: RuntimeException => e.printStackTrace()
      }
  }

}
