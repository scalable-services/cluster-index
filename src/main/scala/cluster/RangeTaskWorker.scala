package cluster

import cluster.grpc._
import com.google.protobuf.ByteString
import com.google.protobuf.any.Any
import io.vertx.core.{Vertx, VertxOptions}
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

  val voptions = new VertxOptions()
  voptions.setBlockedThreadCheckInterval(1000000L)
  val vertx = Vertx.vertx(voptions)

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

    logger.info(s"\n${Console.YELLOW_B}range task for ${task.id}${Console.RESET}\n")

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

    val removals = task.removals.map { c =>
      Commands.Remove[K, V](task.id, c.list.map { case kp =>
        Tuple2(indexBuilder.keySerializer.deserialize(kp.key.toByteArray), Some(kp.version))
      })
    }

    // This order must be mantained...
    val rangeCmds: Seq[Commands.Command[K, V]] = updates ++ insertions ++ removals

    def actionAfterExecution(metaCR: ClusterIndex[K, V], maxRangeK: K): Future[Boolean] = {

      def removeFromMeta(): Future[Boolean] = {
        val metaTask = MetaTask("meta")
          .withRemoveRanges(Seq(ByteString.copyFrom(indexBuilder.keySerializer.serialize(maxRangeK))))

        println(s"${Console.RED_B}SENDING REMOVAL META TASK ${task.id}${Console.RESET}")

        sendTasks(Seq(metaTask))
      }

      def updateOrInsertMeta(metaAfter: Seq[(K, KeyIndexContext, String)]): Future[Boolean] = {

        val idx = metaCR.indexes.find{x => x._2.ctx.indexId == task.id}.get._2
        val indexVersion = idx.ctx.lastChangeVersion
        val hasChanged = task.lastChangeVersion.compareTo(indexVersion) != 0

        // assert(!hasChanged, s"Index structure has changed! task version: ${task.lastChangeVersion} index version: ${indexVersion}")

        if (hasChanged) {
          println(s"\n\n${Console.RED_B}Index structure for ${task.id} has changed! task version: ${task.lastChangeVersion} index version: ${indexVersion}${Console.RESET}\n\n")
        }

        //val maxRangeKAfter = metaAfter.head._1

        val newRanges = metaAfter.length > 1

        if (hasChanged || newRanges) {
          val metaTask = MetaTask("meta")
            .withRemoveRanges(Seq(ByteString.copyFrom(indexBuilder.keySerializer.serialize(maxRangeK))))
            .withInsertRanges(metaAfter.map { case (k, c, _) =>
              KeyIndexContext(ByteString.copyFrom(indexBuilder.keySerializer.serialize(k)), c.indexId)
            })

          println(s"${Console.MAGENTA_B}SENDING UPDATE/INSERT META TASK ${task.id}${Console.RESET}")

          return sendTasks(Seq(metaTask))
        }

        //val nctx = idx.snapshot().withId(task.id)
        //idx.ctx = Context.fromIndexContext[K, V](nctx)(indexBuilder)

        println(s"${Console.GREEN_B}NORMAL OPERATIONS on ${task.id}${Console.RESET}")

        Future.successful(true)
      }

      TestHelper.all(metaCR.meta.inOrder()).flatMap { list =>
        if(list.isEmpty){
          removeFromMeta()
        } else {
          updateOrInsertMeta(list)
        }
      }
    }

    def checkVersions(metaCR: ClusterIndex[K, V]): Future[Boolean] = {
      val idx = metaCR.indexes.head._2
      val indexVersion = idx.ctx.lastChangeVersion
      val hasChanged = task.lastChangeVersion.compareTo(indexVersion) != 0

      // assert(!hasChanged, s"Index structure has changed! task version: ${task.lastChangeVersion} index version: ${indexVersion}")

      // It does not change when running a simulation with only one transaction because each range is only accessed once...
      if (hasChanged) {
        println(s"\n\n${Console.RED_B}Index structure has changed! task version: ${task.lastChangeVersion} index version: ${indexVersion}${Console.RESET}\n\n")
        System.exit(1)
      }

      Future.successful(true)
    }

    for {
      (maxRangeK, metaCR) <- ClusterIndex.fromRange[K, V](task.id, TestHelper.NUM_LEAF_ENTRIES,
        TestHelper.NUM_META_ENTRIES)(indexBuilder, clusterIndexBuilder)

      _ <- checkVersions(metaCR)

      n <- metaCR.execute(rangeCmds).map { r =>
        if(r.error.isDefined) throw r.error.get
        r
      }
      ok <- metaCR.saveIndexes(false)
      result <- actionAfterExecution(metaCR, maxRangeK)
    } yield {
      true
    }
  }

}
