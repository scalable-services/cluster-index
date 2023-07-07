package cluster

import cluster.ClusterSerializers._
import cluster.grpc.{ClusterIndexCommand, KeyIndexContext, MetaTask, RangeTask}
import com.datastax.oss.driver.api.core.CqlSession
import com.google.protobuf.ByteString
import com.google.protobuf.any.Any
import io.vertx.core.Vertx
import io.vertx.kafka.client.consumer.{KafkaConsumer, KafkaConsumerRecords}
import io.vertx.kafka.client.producer.{KafkaProducer, KafkaProducerRecord}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.slf4j.LoggerFactory
import services.scalable.index.DefaultComparators._
import services.scalable.index.DefaultPrinters._
import services.scalable.index.DefaultSerializers._
import services.scalable.index.grpc.IndexContext
import services.scalable.index.impl.{CassandraStorage, DefaultCache}
import services.scalable.index.{Bytes, Context, DefaultComparators, DefaultIdGenerators, DefaultPrinters, DefaultSerializers, IdGenerator, IndexBuilder, Serializer}

import java.util
import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.jdk.FutureConverters._

class RangeTaskWorker(val id: String) {

  val logger = LoggerFactory.getLogger(this.getClass)

  val vertx = Vertx.vertx()
  val consumerSettings = new util.HashMap[String, String]()

  consumerSettings.put("bootstrap.servers", "localhost:9092")
  consumerSettings.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  consumerSettings.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
  consumerSettings.put("group.id", s"range-task-workers")
  consumerSettings.put("auto.offset.reset", "earliest")
  //consumerSettings.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")
  consumerSettings.put("enable.auto.commit", "false")

  val consumer = KafkaConsumer.create[String, Array[Byte]](vertx, consumerSettings)

  val producerSettings = new util.HashMap [String, String] ()
  producerSettings.put("bootstrap.servers", "localhost:9092")
  producerSettings.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  producerSettings.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
  producerSettings.put("acks", "1")

  val producer = KafkaProducer.create[String, Array[Byte]](vertx, producerSettings)

  def handler(records: KafkaConsumerRecords[String, Array[Byte]]): Unit = {
    val msgs = records.records().iterator().asScala.toSeq.map { msg =>
      Any.parseFrom(msg.value()).unpack(RangeTask)
    }

    // Abort changed reads before doing writes
    //TODO: implement if not exists for inserts as well...
    /*val groupedOps = msgs.groupBy { m =>

    }*/

    msgs.foreach { rt =>
      Await.result(insertRange(rt), Duration.Inf)
    }

    consumer.commit().result()
  }

  consumer.handler(_ => {})
  consumer.batchHandler(handler)
  consumer.subscribe(TestConfig.RANGE_INDEX_TOPIC).result()

  type K = Bytes
  type V = Bytes

  implicit val idGenerator = DefaultIdGenerators.idGenerator
  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val cache = new DefaultCache(MAX_PARENT_ENTRIES = 80000)
  //implicit val storage = new MemoryStorage(NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
  implicit val storage = new CassandraStorage(TestConfig.session, false)

  implicit val metaIndexSerializer = new Serializer[IndexContext] {
    override def serialize(t: IndexContext): Bytes = Any.pack(t).toByteArray
    override def deserialize(b: Bytes): IndexContext = Any.parseFrom(b).unpack(IndexContext)
  }

  val indexBuilder = IndexBuilder.create[K, V](DefaultComparators.bytesOrd)
    .storage(storage)
    .serializer(DefaultSerializers.grpcBytesBytesSerializer)
    .keyToStringConverter(DefaultPrinters.byteArrayToStringPrinter)

  val clusterMetaBuilder = IndexBuilder.create[K, KeyIndexContext](DefaultComparators.bytesOrd)
    .storage(storage)
    .serializer(grpcByteArrayKeyIndexContextSerializer)
    .keyToStringConverter(DefaultPrinters.byteArrayToStringPrinter)

  def sendTasks(tasks: Seq[ClusterIndexCommand]): Future[Boolean] = {
    val records = tasks.map {
      case t: MetaTask =>
        KafkaProducerRecord.create[String, Array[Byte]](TestConfig.META_INDEX_TOPIC, Any.pack(t).toByteArray)

      case t: RangeTask =>
        KafkaProducerRecord.create[String, Array[Byte]](TestConfig.RANGE_INDEX_TOPIC, Any.pack(t).toByteArray)
    }

    producer.setWriteQueueMaxSize(records.length)

    records.foreach { msg =>
      producer.send(msg)
    }

    producer.flush().toCompletionStage.asScala.map(_ => true)
  }

  def insertRange(task: RangeTask): Future[Boolean] = {

    logger.info(s"\n${Console.MAGENTA_B}range task for ${task.id}${Console.RESET}\n")

    for {
      (maxRangeK, metaCR) <- ClusterIndex.fromRange(task.id, TestHelper.NUM_LEAF_ENTRIES,
        TestHelper.NUM_META_ENTRIES)(indexBuilder, clusterMetaBuilder)

      rangeCmds = task.commands.map { c =>
        c.list.map { case kp =>
          Tuple3(kp.key.toByteArray, kp.value.toByteArray, true)
        }
      }.flatten.sortBy(_._1)

      n <- metaCR.insert(rangeCmds)
      ok <- metaCR.saveIndexes(false)
      metaAfter <- TestHelper.all(metaCR.meta.inOrder())

      maxRangeKAfter = metaAfter.head._1
      //val rangeCtxAfter = metaAfter.head._2

      firstChanged = !bytesOrd.equiv(maxRangeK, maxRangeKAfter)
      newRanges = metaAfter.length > 1

      result <- if (firstChanged || newRanges) {
        var metaTask = MetaTask("meta")

        metaTask = metaTask
          .withRemoveRanges(Seq(ByteString.copyFrom(maxRangeK)))
          .withInsertRanges(metaAfter.map { case (k, c, _) =>
            KeyIndexContext(ByteString.copyFrom(k), c.ctxId)
          })

        println(s"${Console.RED_B}SENDING META TASK ${task.id}${Console.RESET}")

        sendTasks(Seq(metaTask))
      } else {

        val idx = metaCR.indexes.find { case (k, _) => task.id != k }.get._2
        val nctx = idx.snapshot().withId(task.id)

        idx.ctx = Context.fromIndexContext[Bytes, Bytes](nctx)(indexBuilder)

        idx.save().map { _ =>
          println(s"${Console.GREEN_B}NORMAL INSERTION on ${task.id}${Console.RESET}")
          true
        }

      }

    } yield {
      result
    }
  }

}
