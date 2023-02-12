package cluster

import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.scaladsl.{Committer, Consumer, Producer}
import akka.kafka.{CommitDelivery, CommitterSettings, ConsumerSettings, ProducerSettings, Subscriptions}
import akka.stream.scaladsl.{Sink, Source}
import cluster.grpc.{ClusterIndexCommand, KeyIndexContext, MetaTask, RangeTask}
import com.google.protobuf.ByteString
import com.google.protobuf.any.Any
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer}
import org.slf4j.LoggerFactory
import services.scalable.index.grpc.IndexContext
import services.scalable.index.impl.{CassandraStorage, DefaultCache}
import services.scalable.index.{AsyncIterator, Bytes, Commands, Context, IdGenerator, QueryableIndex, Serializer, Tuple}

import java.util.UUID
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import services.scalable.index.DefaultSerializers._
import services.scalable.index.DefaultComparators._
import cluster.ClusterSerializers._

class RangeTaskWorker(val id: String) {

  val logger = LoggerFactory.getLogger(this.getClass)

  implicit val system = ActorSystem.create()
  implicit val ec = system.dispatcher

  val consumerSettings = ConsumerSettings[String, Array[Byte]](system, new StringDeserializer, new ByteArrayDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId(s"range-task-workers")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    .withClientId(s"range-task-worker-${id}")
    .withPollInterval(java.time.Duration.ofMillis(10L))
    .withStopTimeout(java.time.Duration.ofHours(1))
    //.withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")
  //.withStopTimeout(java.time.Duration.ofSeconds(1000L))

  val committerSettings = CommitterSettings(system).withDelivery(CommitDelivery.waitForAck)

  type K = Bytes
  type V = Bytes

  implicit val idGenerator = new IdGenerator {
    override def generateId[K, V](ctx: Context[K, V]): String = UUID.randomUUID.toString
    override def generatePartition[K, V](ctx: Context[K, V]): String = "p0"
  }

  implicit val cache = new DefaultCache(MAX_PARENT_ENTRIES = 80000)
  //implicit val storage = new MemoryStorage(NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
  implicit val storage = new CassandraStorage("history", false)

  implicit val metaIndexSerializer = new Serializer[IndexContext] {
    override def serialize(t: IndexContext): Bytes = Any.pack(t).toByteArray

    override def deserialize(b: Bytes): IndexContext = Any.parseFrom(b).unpack(IndexContext)
  }

  def all[K, V](it: AsyncIterator[Seq[Tuple[K, V]]])(implicit ec: ExecutionContext): Future[Seq[Tuple[K, V]]] = {
    it.hasNext().flatMap {
      case true => it.next().flatMap { list =>
        all(it).map {
          list ++ _
        }
      }
      case false => Future.successful(Seq.empty[Tuple[K, V]])
    }
  }

  val producerSettings = ProducerSettings[String, Bytes](system, new StringSerializer, new ByteArraySerializer)
    .withBootstrapServers("localhost:9092")

  val kafkaProducer = producerSettings.createKafkaProducer()
  val settingsWithProducer = producerSettings.withProducer(kafkaProducer)

  def sendTasks(tasks: Seq[ClusterIndexCommand]): Future[Boolean] = {
    val records = tasks.map {
      case t: MetaTask => new ProducerRecord[String, Bytes]("meta-index-tasks",
        t.id, Any.pack(t).toByteArray)
      case t: RangeTask => new ProducerRecord[String, Bytes]("range-index-tasks",
        t.id, Any.pack(t).toByteArray)
    }

    Source(records)
      .runWith(Producer.plainSink(settingsWithProducer)).map(_ => true)
  }

  def insertRange(task: RangeTask): Future[Boolean] = {

    logger.info(s"\n${Console.MAGENTA_B}range task for ${task.id}${Console.RESET}\n")

    /*val cmds = task.commands.map { c =>
      Commands.Insert(task.id, c.list.map(x => x.key.toByteArray -> x.value.toByteArray), true)
    }*/

    val rangeIndexCtx = Await.result(storage.loadIndex(task.id), Duration.Inf).get

    val rangeIndex = new QueryableIndex[K, V](rangeIndexCtx)

    val rangeList = Await.result(TestHelper.all(rangeIndex.inOrder()), Duration.Inf)
      .map{case (k, v, _) => k -> v}

    val metaRange = new QueryableIndex[K, KeyIndexContext](IndexContext()
      .withId(UUID.randomUUID.toString)
      .withMaxNItems(Int.MaxValue)
      .withLevels(0)
      .withNumLeafItems(rangeIndexCtx.numLeafItems)
      .withNumMetaItems(rangeIndexCtx.numMetaItems))

    val beforeInsertList = Await.result(TestHelper.all(rangeIndex.inOrder()), Duration.Inf).map{x => new String(x._1)}

    val maxRangeK = Await.result(rangeIndex.last(), Duration.Inf).get.max().get._1

    Await.result(metaRange.insert(Seq(
      maxRangeK -> KeyIndexContext(ByteString.copyFrom(maxRangeK), rangeIndexCtx.id)
    )), Duration.Inf)

    val clusterRangeCtx = metaRange.ctx.snapshot(false)

    val metaCR = new ClusterIndex[K, V](clusterRangeCtx, 256,
      clusterRangeCtx.numLeafItems, clusterRangeCtx.numMetaItems)

    metaCR.indexes.put(rangeIndex.ctx.indexId, rangeIndex)

    val rangeCmds = task.commands.map { c =>
      c.list.map { case kp =>
        kp.key.toByteArray -> kp.value.toByteArray
      }
    }.flatten.sortBy(_._1)

    Await.result(metaCR.insert(rangeCmds).flatMap(_ => metaCR.saveIndexes(false)), Duration.Inf)

    val rangeListAfterInsertion = (rangeList ++ rangeCmds).sortBy(_._1).toList
    val indexAfterInsertion = metaCR.inOrder().map { case (k, v, _) => k -> v }.toList

    println(s"rangeListAfterInsertion ${task.id} ${rangeListAfterInsertion.map{case (k, v) => new String(k)}}")
    println(s"indexAfterInsertion     ${task.id}   ${indexAfterInsertion.map{case (k, v) => new String(k)}}")

    assert(Helper.isColEqual(rangeListAfterInsertion, indexAfterInsertion), "not equal!")

    val indexAfterInsertionFromSaved = metaCR.inOrderFromSaved().map{case (k, v, _) => k -> v}.toList

    println(s"rangeListAfterInsertion      ${task.id}  ${rangeListAfterInsertion.map { case (k, v) => new String(k) }}")
    println(s"indexAfterInsertionFromSaved ${task.id}  ${indexAfterInsertionFromSaved.map { case (k, v) => new String(k) }}")

    assert(Helper.isColEqual(rangeListAfterInsertion, indexAfterInsertionFromSaved))

    val metaAfter = Await.result(TestHelper.all(metaCR.meta.inOrder()), Duration.Inf)

    val maxRangeKAfter = metaAfter.head._1
    val rangeCtxAfter = metaAfter.head._2

    val firstChanged = !ord.equiv(maxRangeK, maxRangeKAfter)
    val newRanges = metaAfter.length > 1

    val metaCtx = Await.result(storage.loadIndex("meta"), Duration.Inf)
    val m = new QueryableIndex[K, V](metaCtx.get)
    val metaKeys = Await.result(TestHelper.all(m.inOrder()), Duration.Inf).map { x => new String(x._1) }

    if(firstChanged || newRanges){

      var metaTask = MetaTask("meta")

      metaTask = metaTask
        .withRemoveRanges(Seq(ByteString.copyFrom(maxRangeK)))
        .withInsertRanges(metaAfter.map { case (k, c, _) =>
          KeyIndexContext(ByteString.copyFrom(k), c.ctxId)
        })

      println(s"${Console.RED_B}SENDING META TASK ${task.id}${Console.RESET}")

      return sendTasks(Seq(metaTask))
    }

    //normal insertion in range (todo)

    println("normal insertion...")

    val idx = metaCR.indexes.find {case (k, _) => task.id != k}.get._2
    val nctx = idx.snapshot().withId(task.id)

    storage.save(nctx, idx.ctx.getBlocks()
      .map { case (id, block) => id -> idx.serializer.serialize(block) }).map { r =>
      idx.ctx.clear()
      true
    }.map { _ =>
      println(s"${Console.GREEN_B}NORMAL INSERTION on ${task.id}${Console.RESET}")

      val ctx = Await.result(storage.loadIndex(task.id), Duration.Inf).get
      val index = new QueryableIndex[K, V](ctx)

      val data = Await.result(TestHelper.all(index.inOrder()), Duration.Inf).map{case (k, v, _) => new String(k)}

      true
    }
  }

  def handler(msg: CommittableMessage[String, Array[Byte]]): Future[Boolean] = {
    val rec = msg.record
    val task = Any.parseFrom(rec.value()).unpack(RangeTask)

    insertRange(task)
  }

  val control = {
    Consumer
      .committableSource(consumerSettings, Subscriptions.topics("range-index-tasks"))
      /*.mapAsync(1) { msg =>
        handler(msg).map(_ => msg.committableOffset)
      }*/
      .map { msg =>
        Await.result(handler(msg), Duration.Inf)
        msg.committableOffset
      }
      //.log("debugging")
      .via(Committer.flow(committerSettings.withMaxBatch(1)))
      /*.toMat(Sink.ignore)(DrainingControl.apply)
      .run().streamCompletion*/
      .runWith(Sink.ignore)
      .recover {
        case e: RuntimeException => e.printStackTrace()
      }
  }

  Await.result(system.whenTerminated.map { _ =>
    storage.close()
  }, Duration.Inf)

}
