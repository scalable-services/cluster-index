package cluster

import cluster.grpc._
import com.google.protobuf.ByteString
import com.google.protobuf.any.Any
import io.vertx.core.Vertx
import io.vertx.kafka.client.consumer.{KafkaConsumer, KafkaConsumerRecords}
import io.vertx.kafka.client.producer.{KafkaProducer, KafkaProducerRecord}
import org.slf4j.LoggerFactory
import services.scalable.index.grpc.IndexContext
import services.scalable.index.{Bytes, Commands, Context, IndexBuilder, Serializer}

import java.util
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.jdk.FutureConverters._

class RangeTaskWorker[K, V](val id: String)(val indexBuilder: IndexBuilder[K, V],
                                            val clusterIndexBuilder: IndexBuilder[K, KeyIndexContext]) {

  import indexBuilder._

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
      Await.result(execute(rt), Duration.Inf)
    }

    consumer.commit().result()
  }

  consumer.handler(_ => {})
  consumer.batchHandler(handler)
  consumer.subscribe(TestConfig.RANGE_INDEX_TOPIC).result()

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

  def execute(task: RangeTask): Future[Boolean] = {

    logger.info(s"\n${Console.MAGENTA_B}range task for ${task.id}${Console.RESET}\n")

    val insertions = task.insertions.map { c =>
      Commands.Insert(task.id, c.list.map { case kp =>
        Tuple3(indexBuilder.keySerializer.deserialize(kp.key.toByteArray), indexBuilder.valueSerializer
          .deserialize(kp.value.toByteArray), c.upsert)
      })
    }

    val updates = task.updates.map { c =>
      Commands.Update(task.id, c.list.map { case kp =>
        Tuple3(indexBuilder.keySerializer.deserialize(kp.key.toByteArray), indexBuilder.valueSerializer
          .deserialize(kp.value.toByteArray), Some(kp.version))
      })
    }

    val rangeCmds: Seq[Commands.Command[K, V]] = insertions ++ updates

    for {
      (maxRangeK, metaCR) <- ClusterIndex.fromRange[K, V](task.id, TestHelper.NUM_LEAF_ENTRIES,
        TestHelper.NUM_META_ENTRIES)(indexBuilder, clusterIndexBuilder)

      n <- metaCR.execute(rangeCmds).map { r =>
        if(r.error.isDefined) throw r.error.get
        r
      }
      ok <- metaCR.saveIndexes(false)

      metaAfter <- TestHelper.all(metaCR.meta.inOrder())

      maxRangeKAfter = metaAfter.head._1
      //val rangeCtxAfter = metaAfter.head._2

      firstChanged = !indexBuilder.ord.equiv(maxRangeK, maxRangeKAfter)
      newRanges = metaAfter.length > 1

      result <- if (firstChanged || newRanges) {
        var metaTask = MetaTask("meta")

        metaTask = metaTask
          .withRemoveRanges(Seq(ByteString.copyFrom(indexBuilder.keySerializer.serialize(maxRangeK))))
          .withInsertRanges(metaAfter.map { case (k, c, _) =>
            KeyIndexContext(ByteString.copyFrom(indexBuilder.keySerializer.serialize(k)), c.ctxId)
          })

        println(s"${Console.RED_B}SENDING META TASK ${task.id}${Console.RESET}")

        sendTasks(Seq(metaTask))
      } else {

        //val idx = metaCR.indexes.find { case (id, _) => task.id != id }.get._2
        val idx = metaCR.indexes.head._2

        val nctx = idx.snapshot().withId(task.id)
        idx.ctx = Context.fromIndexContext[K, V](nctx)(indexBuilder)

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
