package services.scalable.index.cluster

import com.google.protobuf.ByteString
import services.scalable.index.Commands.{Insert, Remove, Update}
import services.scalable.index.Errors.IndexError
import services.scalable.index.cluster.ClusterResult.{BatchResult, InsertionResult}
import services.scalable.index.cluster.grpc.KeyIndexContext
import services.scalable.index.{Commands, Errors, IndexBuilt, QueryableIndex}
import services.scalable.index.grpc.{IndexContext, RootRef}

import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

final class ClusterIndex[K, V](val metaDescriptor: IndexContext)
                              (implicit val rangeBuilder: IndexBuilt[K, V],
                               val clusterBuilder: IndexBuilt[K, KeyIndexContext]) {

  case class KeySearchResult(lastKey: K, lastVersion: String, range: LeafRange[K, V], rctx: KeyIndexContext)

  import clusterBuilder._

  assert(rangeBuilder.MAX_N_ITEMS > 0)
  assert(rangeBuilder.MAX_META_ITEMS > 0)
  assert(rangeBuilder.MAX_LEAF_ITEMS > 0)

  def save(): Future[IndexContext] = {
    meta.save().flatMap { metaCtx =>
      Future.sequence(ranges.map{case (rid, range) => range.save()}).map { _ =>
        metaCtx
      }
    }
  }

  // Meta should be instantiated again in case of error inserting on it
  val meta = new QueryableIndex[K, KeyIndexContext](metaDescriptor)(clusterBuilder)
  val ranges = new TrieMap[String, LeafRange[K, V]]()
  val newRanges = new TrieMap[String, LeafRange[K, V]]()

  protected def getRange(id: String): Future[Option[LeafRange[K, V]]] = {
    ranges.get(id) match {
      case None =>

        println(s"did no find range ${id}... trying storage...")

        storage.loadIndex(id).map {
          case None => None
          case Some(ctx) =>

            val range = new LeafRange[K, V](ctx)(rangeBuilder)
            ranges.putIfAbsent(id, range)
            Some(range)
      }
      case someRange =>

        println(s"found range ${id} in cache...")

        Future.successful(someRange)
    }
  }

  protected def findPath(k: K): Future[Option[KeySearchResult]] = {
    meta.find(k).flatMap {
      case Some(leaf) =>
        val (_, (lastKey, rctx, lastVersion)) = leaf.findPath(k)

        getRange(rctx.rangeId).map {
          case Some(range) => Some(KeySearchResult(lastKey, lastVersion, range, rctx))
          case None => None
        }

      case None => Future.successful(None)
    }
  }

  def insertEmpty(data: Seq[(K, V, Boolean)], insertVersion: String): Future[InsertionResult] = {

    val maxN = Math.min(data.length, rangeBuilder.MAX_N_ITEMS).toInt
    val slice = data.slice(0, maxN)

    val rangeCtx = IndexContext()
      .withId(UUID.randomUUID().toString)
      .withNumElements(maxN)
      .withMaxNItems(rangeBuilder.MAX_N_ITEMS)
      .withNumLeafItems(rangeBuilder.MAX_LEAF_ITEMS)
      .withNumMetaItems(rangeBuilder.MAX_META_ITEMS)
      .withLastChangeVersion(UUID.randomUUID().toString)

    val range = new LeafRange[K, V](rangeCtx)(rangeBuilder)

    storage.createIndex(rangeCtx).flatMap { r =>
      range.execute(Seq(Insert(rangeCtx.id, slice, Some(insertVersion))), insertVersion).flatMap {
        case cr if cr.success => range.max().map(_.get).flatMap { case (maxKey, _, maxLastV) =>

          //val metaCopy = new QueryableIndex[K, KeyIndexContext](metaDescriptor)(clusterBuilder)

          meta.execute(Seq(
            Insert(meta.ctx.indexId, Seq(Tuple3(maxKey,
              KeyIndexContext()
                .withLastChangeVersion(rangeCtx.lastChangeVersion)
                .withRangeId(rangeCtx.id)
                .withKey(ByteString.copyFrom(rangeBuilder.keySerializer.serialize(maxKey))), false)), Some(insertVersion))
          )).map { mbr =>

            newRanges.put(range.ctx.indexId, range)
            ranges.put(range.ctx.indexId, range)

            assert(range.ctx.root.isDefined)
            assert(range.ctx.num_elements == Await.result(range.length(), Duration.Inf))

            InsertionResult(cr.success, cr.n, mbr.error)
          }
        }
        case cr => Future.successful(InsertionResult(cr.success, cr.n, cr.error))
      }
    }
  }

  def insertRange(sr: KeySearchResult, data: Seq[(K, V, Boolean)], insertVersion: String): Future[InsertionResult] = {

    assert(Await.result(sr.range.length(), Duration.Inf) == sr.range.ctx.num_elements, (sr.rctx.rangeId,
      Await.result(sr.range.length(), Duration.Inf), sr.range.ctx.num_elements))

    sr.range.copy(true).map(_.asInstanceOf[LeafRange[K, V]]).flatMap { range =>
      range.isFull().flatMap {
        case false =>

          assert(data.length > 0)

          val maxN = Math.min(rangeBuilder.MAX_N_ITEMS - range.ctx.num_elements, data.length).toInt
          val slice = data.slice(0, maxN)

          if(maxN <= 0){
            println(s"MAXN to INSERT: ${maxN} MAX_N_ITEMS: ${rangeBuilder.MAX_N_ITEMS} num_elems: ${range.ctx.num_elements}")
            assert(false)
          }

          range.insert(slice, insertVersion).map {
            case cr if cr.success =>

            ranges.update(sr.rctx.rangeId, range)

              assert(range.ctx.root.isDefined)
              assert(range.ctx.num_elements == Await.result(range.length(), Duration.Inf))

            InsertionResult(cr.success, cr.n, cr.error)

            case cr => InsertionResult(cr.success, cr.n, cr.error)
        }

        case true => range.split().map(_.asInstanceOf[LeafRange[K, V]]).flatMap { right =>
          range.max().map(_.get).flatMap { rangeMax =>
            right.max().map(_.get).flatMap { rightMax =>

             // val metaCopy = new QueryableIndex[K, KeyIndexContext](metaDescriptor)(clusterBuilder)

              storage.createIndex(right.ctx.currentSnapshot()).flatMap { r =>
                meta.execute(Seq(
                  Remove(meta.ctx.indexId, Seq(
                    sr.lastKey -> Some(sr.lastVersion)
                  ), Some(insertVersion)),

                  Insert(meta.ctx.indexId, Seq(
                    (rangeMax._1, KeyIndexContext()
                      .withRangeId(sr.rctx.rangeId)
                      .withLastChangeVersion(range.ctx.lastChangeVersion)
                      .withKey(ByteString.copyFrom(rangeBuilder.keySerializer.serialize(rangeMax._1))), false),
                    (rightMax._1, KeyIndexContext()
                      .withRangeId(right.ctx.indexId)
                      .withLastChangeVersion(right.ctx.lastChangeVersion)
                      .withKey(ByteString.copyFrom(rangeBuilder.keySerializer.serialize(rightMax._1))), false)
                  ), Some(insertVersion))
                )).map {
                  case mbr if mbr.success =>

                    // meta = metaCopy
                    val all = meta.allSync()

                    ranges.update(sr.rctx.rangeId, range)
                    ranges.put(right.ctx.indexId, right)

                    newRanges.put(right.ctx.indexId, right)

                    assert(right.ctx.root.isDefined)
                    assert(right.ctx.num_elements == Await.result(right.length(), Duration.Inf))

                    assert(range.ctx.root.isDefined)
                    assert(range.ctx.num_elements == Await.result(range.length(), Duration.Inf))

                    InsertionResult(mbr.success, 0, None)

                  case mbr => InsertionResult(mbr.success, 0, mbr.error)
                }
              }

            }
          }
        }
      }
    }
  }

  def insert(data: Seq[(K, V, Boolean)], insertVersion: String): Future[InsertionResult] = {
    val sorted = data.sortBy(_._1)

    if(sorted.exists{case (k, _, _) => sorted.count{case (k1, _, _) => ord.equiv(k, k1)} > 1}){
      return Future.successful(InsertionResult(false, 0,
        Some(Errors.DUPLICATED_KEYS(data.map(_._1), rangeBuilder.ks))))
    }

    val len = sorted.length
    var pos = 0

    def insert(): Future[Int] = {
      if(pos == len) return Future.successful(sorted.length)

      var list = sorted.slice(pos, len)
      val (k, _, _) = list(0)

      findPath(k).flatMap {
        case None => insertEmpty(list, insertVersion)
        case Some(sr: KeySearchResult) =>

          val idx = list.indexWhere{case (k, _, _) => ord.gt(k, sr.lastKey)}
          if(idx > 0) list = list.slice(0, idx)

          insertRange(sr, list, insertVersion)
      }.flatMap { r =>
        pos += r.n
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

  def execute(commands: Seq[Commands.Command[K, V]], version: String): Future[ClusterResult.BatchResult] = {

    def process(pos: Int, error: Option[Throwable], n: Int): Future[BatchResult] = {
      if(error.isDefined) {
        return Future.successful(BatchResult(false, error))
      }

      if(pos == commands.length) {
        return Future.successful(BatchResult(true, None, n))
      }

      val cmd = commands(pos)

      (cmd match {
        case cmd: Insert[K, V] => insert(cmd.list, cmd.version.getOrElse(version))
        //case cmd: Remove[K, V] => remove(cmd.keys, cmd.version.getOrElse(version))
        //case cmd: Update[K, V] => update(cmd.list, cmd.version.getOrElse(version))
      }).flatMap(prev => process(pos + 1, prev.error, prev.n))
    }

    process(0, None, 0)
  }

  def inOrderSync(): Seq[(K, V, String)] = {
    meta.allSync().map { case (k, rctx, lastV) =>
      Await.result(getRange(rctx.rangeId).map {
        case None =>
          Seq.empty[(K, V, String)]
        case Some(range) =>

          println(s"rangeId: ${rctx.rangeId}, n: ${range.ctx.num_elements}, max: ${rangeBuilder.MAX_N_ITEMS}")

          range.inOrderSync()
      }, Duration.Inf)
    }.flatten
  }

}
