package cluster

import akka.actor.ActorSystem
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.Source
import cluster.grpc.{ClusterIndexCommand, KeyIndexContext, MetaTask, RangeTask}
import com.google.protobuf.any.Any
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import services.scalable.index.{Block, Bytes, Cache, Commands, DefaultComparators, DefaultPrinters, DefaultSerializers, IdGenerator, IndexBuilder, QueryableIndex, Serializer, Storage}
import services.scalable.index.grpc.IndexContext

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import Printers._
import services.scalable.index.impl.GrpcByteSerializer
import ClusterSerializers._

class ClusterClient[K, V](metaCtx: IndexContext)(val metaBuilder: IndexBuilder[K, KeyIndexContext]){

  import metaBuilder._

  val system = ActorSystem.create()
  implicit val provider = system.classicSystem

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

  val meta = new QueryableIndex[K, KeyIndexContext](metaCtx)(metaBuilder)
  //meta.createIndex("main", NUM_LEAF_ENTRIES, NUM_META_ENTRIES)

  def findRange(k: K): Future[Option[(K, IndexContext, String)]] = {
    meta.findPath(k).map {
      case None => None
      case Some(leaf) => Some(leaf.findPath(k)._2).map { x =>
        (x._1, Await.result(storage.loadIndex(x._2.ctxId), Duration.Inf).get, x._3)
      }
    }
  }

  def sliceInsertion(c: Commands.Insert[K, V])(ranges: TrieMap[String, Seq[Commands.Command[K, V]]]): Future[Int] = {
    val sorted = c.list.sortBy(_._1)

    val len = sorted.length
    var pos = 0

    //val insertions = TrieMap.empty[String, Seq[Commands.Insert[K, V]]]

    def insert(pos: Int): Future[Int] = {
      if (pos == len) return Future.successful(sorted.length)

      var list = sorted.slice(pos, len)
      val (k, _, vs) = list(0)

      //println(s"key to find: ${new String(k.asInstanceOf[Bytes])}")

      findRange(k).map {
        case None =>
          //println("none")
          list.length
        case Some((last, dbCtx, version)) =>

          val idx = list.indexWhere { case (k, _, _) => ord.gt(k, last) }
          if (idx > 0) list = list.slice(0, idx)

          //println(s"${Thread.currentThread().threadId()} seeking range for slice from ${pos}: ${list.map{x => new String(x._1.asInstanceOf[Bytes])}}")

          val c = Commands.Insert[K, V](dbCtx.id, list)

          ranges.get(dbCtx.id) match {
            case None => ranges.put(dbCtx.id, Seq(c))
            case Some(cmdList) => ranges.update(dbCtx.id, cmdList :+ c)
          }

          list.length
      }.flatMap { n =>
        assert(n > 0)
        //println(s"passed ${Thread.currentThread().threadId()} ${n}...")
       // pos += n
        insert(pos + n)
      }
    }

    insert(0)
  }

  def sliceRemoval(c: Commands.Remove[K, V])(ranges: TrieMap[String, Seq[Commands.Command[K, V]]]): Future[Int] = {
    val sorted = c.keys.sorted

    val len = sorted.length
    var pos = 0

    //val insertions = TrieMap.empty[String, Seq[Commands.Insert[K, V]]]

    def remove(): Future[Int] = {
      if (pos == len) return Future.successful(sorted.length)

      var list = sorted.slice(pos, len)
      val (k, _) = list(0)

      findRange(k).map {
        case None => list.length
        case Some((last, leafId, version)) =>

          val idx = list.indexWhere { case (k, vs) => ord.gt(k, last) }
          if (idx > 0) list = list.slice(0, idx)

          val c = Commands.Remove[K, V]("main", list)

          ranges.get(leafId.id) match {
            case None => ranges.put(leafId.id, Seq(c))
            case Some(cmdList) => ranges.update(leafId.id, cmdList :+ c)
          }

          list.length
      }.flatMap { n =>
        pos += n
        remove()
      }
    }

    remove()
  }

  def sliceUpdate(c: Commands.Update[K, V])(ranges: TrieMap[String, Seq[Commands.Command[K, V]]]): Future[Int] = {
    val sorted = c.list.sortBy(_._1)

    val len = sorted.length
    var pos = 0

    //val insertions = TrieMap.empty[String, Seq[Commands.Insert[K, V]]]

    def update(): Future[Int] = {
      if (pos == len) return Future.successful(sorted.length)

      var list = sorted.slice(pos, len)
      val (k, _, _) = list(0)

      findRange(k).map {
        case None =>
          println("none")
          list.length
        case Some((last, leafId, version)) =>

          val idx = list.indexWhere { case (k, _, _) => ord.gt(k, last) }
          if (idx > 0) list = list.slice(0, idx)

          val c = Commands.Update[K, V]("main", list)

          ranges.get(leafId.id) match {
            case None => ranges.put(leafId.id, Seq(c))
            case Some(cmdList) => ranges.update(leafId.id, cmdList :+ c)
          }

          list.length
      }.flatMap { n =>
        pos += n
        update()
      }
    }

    update()
  }

  def execute(commands: Seq[Commands.Command[K, V]]): Future[TrieMap[String, Seq[Commands.Command[K, V]]]] = {
    val ranges = TrieMap.empty[String, Seq[Commands.Command[K, V]]]

    /*Future.sequence(commands.map {
      case c: Commands.Insert[K, V] => sliceInsertion(c)(ranges)
      /*case c: Commands.Remove[K, V] => sliceRemoval(c)(ranges)
      case c: Commands.Update[K, V] => sliceUpdate(c)(ranges)*/
    }).map { _ =>
      ranges.toMap
    }*/

    sliceInsertion(commands.head.asInstanceOf[Commands.Insert[K, V]])(ranges).map(_ => ranges)
  }

}
