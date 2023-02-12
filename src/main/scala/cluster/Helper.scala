package cluster

import cluster.grpc.tests.ListIndex
import com.datastax.oss.driver.api.core.CqlSession
import com.google.protobuf.ByteString
import com.google.protobuf.any.Any
import services.scalable.index.grpc.{IndexContext, TemporalContext}
import services.scalable.index.{AsyncIterator, Bytes, Storage, Tuple}

import java.nio.ByteBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.CollectionHasAsScala

object Helper {

  def insertListIndex(id: String, data: Seq[(Bytes, Bytes)], session: CqlSession): Boolean = {
    val listIndex = ListIndex(id, data.map { case (k, _) =>
      ByteString.copyFrom(k)
    })

    val serializedListIndex = Any.pack(listIndex).toByteArray

    val ustm = session.prepare(s"INSERT INTO history.test_indexes(id, data) values (?, ?);")
    val bind = ustm.bind(id, ByteBuffer.wrap(serializedListIndex))

    //bind.setString(0, id)
    //bind.setByteBuffer("data", ByteBuffer.wrap(serializedListIndex))

    session.execute(bind).wasApplied()
  }

  def loadListIndex(id: String, session: CqlSession): Option[ListIndex] = {
    val ustm = session.prepare(s"select * from test_indexes where id = ?;")
    val bind = ustm.bind(id)

   // bind.setString("id", id)

    session.execute(bind).all().asScala.map { r =>
      Any.parseFrom(r.getByteBuffer("data").flip().array()).unpack(ListIndex)
    }.headOption
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

  def all[K, V](it: AsyncIterator[Seq[Tuple[K, V]]])(implicit ec: ExecutionContext): Future[Seq[Tuple[K, V]]] = {
    it.hasNext().flatMap {
      case true => it.next().flatMap { list =>
        all(it).map{list ++ _}
      }
      case false => Future.successful(Seq.empty[Tuple[K, V]])
    }
  }

  def isColEqual[K, V](source: Seq[Tuple2[K, V]], target: Seq[Tuple2[K, V]])(implicit ordk: Ordering[K], ordv: Ordering[V]): Boolean = {
    if(target.length != source.length) return false
    for(i<-0 until source.length){
      val (ks, vs) = source(i)
      val (kt, vt) = target(i)

      if(!(ordk.equiv(kt, ks) && ordv.equiv(vt, vs))){
        return false
      }
    }

    true
  }

}
