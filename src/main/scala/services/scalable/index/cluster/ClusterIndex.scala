package services.scalable.index.cluster

import services.scalable.index.cluster.grpc.KeyIndexContext
import services.scalable.index.grpc.{IndexContext, RootRef}
import services.scalable.index.{Commands, Errors, IndexBuilder, IndexBuilt, QueryableIndex, Tuple}
import ClusterResult._
import com.google.protobuf.ByteString
import services.scalable.index.Errors.IndexError
import services.scalable.index.impl.MemoryStorage

import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

final class ClusterIndex[K, V](val descriptor: IndexContext)
                              (implicit val rangeBuilder: IndexBuilt[K, V]) {

  implicit val clusterBuilder = IndexBuilder
    .create[K, KeyIndexContext](rangeBuilder.ec, rangeBuilder.ord,
      descriptor.numLeafItems, descriptor.numMetaItems, descriptor.maxNItems,
      rangeBuilder.keySerializer, ClusterSerializers.keyIndexSerializer)
    .storage(rangeBuilder.storage)
   // .cache(rangeBuilder.cache)
    .build()

  import clusterBuilder._

  val meta = new QueryableIndex[K, KeyIndexContext](descriptor)(clusterBuilder)
  val ranges = TrieMap.empty[String, Range[K, V]]
  val toRemove = TrieMap.empty[String, String]

  def save(): Future[IndexContext] = {

    /*toRemove.foreach { r =>
      ranges.remove(r._1)
    }*/

    Future.sequence(ranges.map(_._2.save())).flatMap { allOk =>
      meta.save()
    }
  }

  def getRange(id: String): Future[Range[K, V]] = {
    ranges.get(id) match {
      case None => clusterBuilder.storage.loadIndex(id)
        .map{ctx =>new Range[K, V](ctx.get)(rangeBuilder)}
      case Some(index) => Future.successful(index)
    }
  }

  def findPath(k: K): Future[Option[Tuple[K, KeyIndexContext]]] = {
    meta.find(k).map {
      case Some(leaf) => Some(leaf.findPath(k)._2)
      case None => None
    }
  }

  def insertEmpty(data: Seq[Tuple3[K, V, Boolean]], insertVersion: String): Future[Int] = {
    val maxItemsToInsert = Math.min(data.length, rangeBuilder.MAX_N_ITEMS).toInt
    val slice = data.slice(0, maxItemsToInsert)

    val ctx = IndexContext()
      .withId(UUID.randomUUID().toString)
      .withLevels(1)
      .withNumElements(0L)
      .withMaxNItems(rangeBuilder.MAX_N_ITEMS)
      .withNumLeafItems(rangeBuilder.MAX_LEAF_ITEMS)
      .withNumMetaItems(rangeBuilder.MAX_META_ITEMS)
      .withLastChangeVersion(UUID.randomUUID().toString)

    val range = new Range[K, V](ctx)(rangeBuilder)

    range.execute(Seq(
      Commands.Insert(range.ctx.indexId, slice, Some(insertVersion))
    )).flatMap { r =>

      assert(r.success)

      ranges.put(range.ctx.indexId, range)

      range.max().map(_.get).flatMap { case (lastKey, _, _) =>
        val kctx = KeyIndexContext()
          .withRangeId(range.ctx.indexId)
          .withLastChangeVersion(range.ctx.lastChangeVersion)
          .withKey(ByteString.copyFrom(rangeBuilder.keySerializer.serialize(lastKey)))

        meta.execute(Seq(
          Commands.Insert(meta.ctx.indexId, Seq(Tuple3(lastKey, kctx, true)), Some(insertVersion))
        )).map(_ => slice.length)
      }
    }
  }

  def insertLeaf(lastKey: K, kctx: KeyIndexContext, lastVersion: String, data: Seq[(K, V, Boolean)],
                 insertVersion: String): Future[Int] = {

    def split(range: Range[K, V]): Future[Int] = {
      range.split().flatMap { right =>
        range.max().map(_.get).flatMap { case (lMax, _, _) =>

          val lOrder = Await.result(range.inOrder(), Duration.Inf)
          val rOrder = Await.result(right.inOrder(), Duration.Inf)

          val all = lOrder ++ rOrder

          ranges.put(right.ctx.indexId, right)
          ranges.update(range.ctx.indexId, range)

          right.max().map(_.get).flatMap { case (rMax, _, _) =>
            val rkctx = KeyIndexContext()
              .withRangeId(right.ctx.indexId)
              .withLastChangeVersion(right.ctx.lastChangeVersion)
              .withKey(ByteString.copyFrom(rangeBuilder.keySerializer.serialize(rMax)))

            val lkctx = KeyIndexContext()
              .withRangeId(range.ctx.indexId)
              .withLastChangeVersion(range.ctx.lastChangeVersion)
              .withKey(ByteString.copyFrom(rangeBuilder.keySerializer.serialize(lMax)))

            meta.execute(Seq(
              Commands.Remove(meta.ctx.indexId, Seq(lastKey -> Some(lastVersion)), Some(insertVersion)),
              Commands.Insert(meta.ctx.indexId, Seq(
                Tuple3(lMax, lkctx, true),
                Tuple3(rMax, rkctx, true)
              ), Some(insertVersion))
            )).map(_ => 0)
          }

        }
      }
    }

    getRange(kctx.rangeId).flatMap(_.copy(sameId = true)).flatMap(r => r.isFull().map(_ -> r)).flatMap {
      case (false, range) =>

        range.insert(data, insertVersion).map { r =>
          ranges.update(range.ctx.indexId, range)
          r.n
        }

      case (true, range) => split(range)
    }
  }

  def insert(data: Seq[Tuple3[K, V, Boolean]], insertVersion: String): Future[InsertionResult] = {
    val sorted = data.sortBy(_._1)

    if(sorted.exists{case (k, _, _) => sorted.count{case (k1, _, _) => ord.equiv(k, k1)} > 1}){
      return Future.successful(InsertionResult(false, 0,
        Some(Errors.DUPLICATED_KEYS(data.map(_._1), clusterBuilder.ks))))
    }

    val len = sorted.length
    var pos = 0

    def insert(): Future[Int] = {
      if(pos == len) return Future.successful(sorted.length)

      var list = sorted.slice(pos, len)
      val (k, _, _) = list(0)

      findPath(k).flatMap {
        case None => insertEmpty(list, insertVersion)
        case Some((last, kctx, lastVersion)) =>

          val idx = list.indexWhere{case (k, _, _) => ord.gt(k, last)}
          if(idx > 0) list = list.slice(0, idx)

          insertLeaf(last, kctx, lastVersion, list, insertVersion)
      }.flatMap { n =>
        pos += n
        //ctx.num_elements += n
        insert()
      }
    }

    insert().map { n =>
      InsertionResult(true, n)
    }.recover {
      case t: IndexError => InsertionResult(false, 0, Some(t))
      case t: Throwable => throw t
    }
  }

  def inOrder(): Seq[Tuple[K, V]] = {
    Await.result(meta.all(
      meta.inOrder()
    ).map { all =>

      all.map { case (lastKey, kctx, lastVersion) =>
        val range = Await.result(getRange(kctx.rangeId), Duration.Inf)
        val list = Await.result(range.inOrder(), Duration.Inf)

        println(s"range id: ${range.ctx.indexId} | len: ${range.ctx.num_elements} list len: ${list.length}")

        list
      }.flatten
    }, Duration.Inf)
  }

}
