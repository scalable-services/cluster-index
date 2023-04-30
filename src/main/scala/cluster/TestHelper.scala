package cluster

import cluster.grpc.KeyIndexContext
import com.google.common.base.Charsets
import services.scalable.index.{AsyncIterator, Block, Bytes, Cache, IdGenerator, QueryableIndex, Serializer, Storage, Tuple}
import services.scalable.index.grpc.{IndexContext, TemporalContext}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import services.scalable.index.DefaultPrinters._

object TestHelper {

  val NUM_LEAF_ENTRIES = 8 //rand.nextInt(5, 64)
  val NUM_META_ENTRIES = 8 //rand.nextInt(5, 64)
  val MAX_ITEMS = 1024

  def loadIndexInOrder(id: String)(implicit ec: ExecutionContext,
                                         storage: Storage,
                                         serializer: Serializer[Block[Bytes, Bytes]],
                                         cache: Cache,
                                         ord: Ordering[Bytes],
                                         idGenerator: IdGenerator): Seq[(String, KeyIndexContext)] = {
    val metaCtx = Await.result(storage.loadIndex(id), Duration.Inf)
    val m = new QueryableIndex[Bytes, Bytes](metaCtx.get)
    val metaKeys = Await.result(TestHelper.all(m.inOrder()), Duration.Inf).map { x => new String(x._1, Charsets.UTF_8) ->
    KeyIndexContext.parseFrom(x._2)}

    metaKeys
  }

  def loadIndexInOrder(ctx: IndexContext)(implicit ec: ExecutionContext,
                                   storage: Storage,
                                   serializer: Serializer[Block[Bytes, Bytes]],
                                   cache: Cache,
                                   ord: Ordering[Bytes],
                                   idGenerator: IdGenerator): Seq[String] = {
    val m = new QueryableIndex[Bytes, Bytes](ctx)
    val metaKeys = Await.result(TestHelper.all(m.inOrder()), Duration.Inf).map { x =>
      new String(x._1, Charsets.UTF_8)
    }

    metaKeys
  }

  def loadOrCreateTemporalIndex(tctx: TemporalContext)(implicit storage: Storage, ec: ExecutionContext): Future[Option[TemporalContext]] = {
    storage.loadTemporalIndex(tctx.id).flatMap {
      case None => storage.createTemporalIndex(tctx).map(_ => Some(tctx))
      case Some(t) => Future.successful(Some(t))
    }
  }

  def loadOrCreateIndex(tctx: IndexContext)(implicit storage: Storage, ec: ExecutionContext): Future[Option[IndexContext]] = {
    storage.loadIndex(tctx.id).flatMap {
      case None => storage.createIndex(tctx).map(_ => Some(tctx))
      case Some(t) => Future.successful(Some(t))
    }
  }

  def loadIndex(id: String)(implicit storage: Storage, ec: ExecutionContext): Future[Option[IndexContext]] = {
    storage.loadIndex(id).flatMap {
      case None => Future.successful(None)
      case Some(t) => Future.successful(Some(t))
    }
  }

  def all[K, V](it: AsyncIterator[Seq[Tuple[K, V]]])(implicit ec: ExecutionContext): Future[Seq[Tuple[K, V]]] = {
    it.hasNext().flatMap {
      case true => it.next().flatMap { list =>
        all(it).map{list ++ _}
      }
      case false => Future.successful(Seq.empty[Tuple[K, V]])
    }
  }

  def isColEqual[K, V](source: Seq[Tuple3[K, V, String]], target: Seq[Tuple3[K, V, String]])(implicit ordk: Ordering[K], ordv: Ordering[V]): Boolean = {
    if(target.length != source.length) return false
    for(i<-0 until source.length){
      val (ks, vs, _) = source(i)
      val (kt, vt, _) = target(i)

      if(!(ordk.equiv(kt, ks) && ordv.equiv(vt, vs))){
        return false
      }
    }

    true
  }

}
