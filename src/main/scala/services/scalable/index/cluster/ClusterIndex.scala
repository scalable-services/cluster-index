package services.scalable.index.cluster

import com.google.protobuf.ByteString
import services.scalable.index.Commands.{Insert, Remove, Update}
import services.scalable.index.Errors.IndexError
import services.scalable.index.cluster.ClusterResult.{BatchResult, InsertionResult, RemovalResult}
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
      Future.sequence(newRanges.map{case (rid, range) => storage.createIndex(range.ctx.currentSnapshot())}).flatMap { _ =>
        Future.sequence(ranges.map{case (rid, range) => range.save()}).map { _ =>
          metaCtx
        }
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

  protected def getRightRange(k: K): Future[Option[(Range[K, V], K, KeyIndexContext, String)]] = {

    val metaKeys = Await.result(meta.all(meta.inOrder()), Duration.Inf)
    val nkpos = metaKeys.indexWhere(x => rangeBuilder.ord.equiv(x._1, k))
    val nk = if(nkpos == metaKeys.length - 1) None else Some(metaKeys(nkpos + 1))

    meta.nextKey(k)(rangeBuilder.ord).flatMap {
      case None =>
        assert(nk.isEmpty)

        Future.successful(None)
      case Some((knext, kctx, lastK)) =>

        assert(nk.isDefined && rangeBuilder.ord.equiv(knext, nk.get._1))

        /* if(!ranges.isDefinedAt(kctx.rangeId)){
           assert(false)
         }*/

        /*storage.loadIndex(kctx.rangeId)*/

        getRange(kctx.rangeId).map(_.get).flatMap(_.copy(true)).map{r =>

          Some((r, knext, kctx, lastK))
        }
      /*.map(_.get)
      .map { c =>
      Some((new Range[K, V](c)(rangeBuilder), knext, kctx, lastK))*/

    }
  }

  protected def getLeftRange(k: K): Future[Option[(Range[K, V], K, KeyIndexContext, String)]] = {

    val metaKeys = Await.result(meta.all(meta.inOrder()), Duration.Inf)
    val nkpos = metaKeys.indexWhere(x => rangeBuilder.ord.equiv(x._1, k))
    val kp = if(nkpos == 0) None else Some(metaKeys(nkpos - 1))

    meta.previousKey(k)(rangeBuilder.ord).flatMap {
      case None => Future.successful(None)
      case Some((kprev, kctx, lastK)) =>

        assert(kp.isDefined && rangeBuilder.ord.equiv(kprev, kp.get._1))

        /*if(!ranges.isDefinedAt(kctx.rangeId)){
          assert(false)
        }*/

        /*storage.loadIndex(kctx.rangeId)*/

        // Copying here is crucial to work correctly
        getRange(kctx.rangeId).map(_.get).flatMap(_.copy(true)).map{ r =>

          Some((r, kprev, kctx, lastK))
        }

      /*storage.loadIndex(kctx.rangeId)
        .map(_.get).map { c =>
          Some((new Range[K, V](c)(rangeBuilder), kprev, kctx, lastK))
        }*/
    }
  }

  private def handleNoMinimum(range: LeafRange[K, V],
                                   lastKey: K, kctx: KeyIndexContext, lastVersion: String,
                              removalVersion: String, n: Int): Future[RemovalResult] = {

    if(range.ctx.num_elements == 0){
      return meta.execute(Seq(
        Remove(kctx.rangeId, Seq(lastKey -> Some(lastVersion))
      ))).map { cr =>

        assert(cr.success)

        ranges.remove(kctx.rangeId)

        RemovalResult(true, n, None)
      }
    }

    def merge(merger: Option[(Range[K, V], K, KeyIndexContext, String)]): Future[RemovalResult] = {
      merger.get._1.merge(range, removalVersion).map(_.asInstanceOf[LeafRange[K, V]]).flatMap { merged =>
        merged.max().map(_.get).flatMap { mergedMax =>
          meta.execute(Seq(
            Remove(kctx.rangeId, Seq(lastKey -> Some(lastVersion),
              merger.get._2 -> Some(merger.get._4)),
              Some(removalVersion)),

            Insert(merged.ctx.indexId, Seq((mergedMax._1,
              KeyIndexContext()
              .withLastChangeVersion(merged.ctx.lastChangeVersion)
              .withRangeId(merged.ctx.indexId)
              .withKey(ByteString.copyFrom(rangeBuilder.keySerializer.serialize(mergedMax._1))), false)),
              Some(removalVersion))
          )).map { cr =>

            assert(cr.success)

            ranges.remove(kctx.rangeId)
            ranges.put(merged.ctx.indexId, merged)

            RemovalResult(true, n, None)
          }
        }
      }
    }

    def choose(leftRange: Option[(Range[K, V], K, KeyIndexContext, String)],
               righRange: Option[(Range[K, V], K, KeyIndexContext, String)]): Future[RemovalResult] = {

      if(leftRange.isDefined && leftRange.get._1.canMerge(range)) {
        println(s"merging with left...")
        return merge(leftRange)
      }

      if(righRange.isDefined && righRange.get._1.canMerge(range)){
        println(s"merging with right...")
        return merge(righRange)
      }

      println("no one to merge with...")

      ranges.update(range.ctx.indexId, range)

      Future.successful(RemovalResult(true, n, None))
    }

    for {
      leftRange <- getLeftRange(lastKey)
      rightRange <- getRightRange(lastKey)
      result <- choose(leftRange, rightRange)
    } yield {
      result
    }
  }

  protected def removeFromLeaf(lastKey: K, kctx: KeyIndexContext, lastVersion: String, keys: Seq[(K, Option[String])],
                               removalVersion: String): Future[RemovalResult] = {
    // Remember to copy the range when altering it...
    getRange(kctx.rangeId).map(_.get).flatMap(_.copy(true)).map(_.asInstanceOf[LeafRange[K, V]]).flatMap { range =>
      range.execute(Seq(Commands.Remove(kctx.rangeId, keys, Some(removalVersion))), removalVersion).flatMap {
        case r if r.success =>
          range.hasMinimum().flatMap {
            case true =>

              println("simple removal...")

              ranges.update(range.ctx.indexId, range)

              Future.successful(RemovalResult(true, keys.length, None))

            case false => handleNoMinimum(range, lastKey, kctx, lastVersion, removalVersion, keys.length)
          }

        case r =>
          Future.failed(r.error.get)
      }
    }
  }

  def remove(data: Seq[Tuple2[K, Option[String]]], removalVersion: String): Future[RemovalResult] = {
    val sorted = data.sortBy(_._1).distinct

    val len = sorted.length
    var pos = 0

    def remove(): Future[Int] = {
      if(pos == len) return Future.successful(sorted.length)

      var list = sorted.slice(pos, len)
      val (k, _) = list(0)

      findPath(k).flatMap {
        case None =>
          Future.failed(Errors.KEY_NOT_FOUND[K](k, rangeBuilder.ks))
        case Some(sr) =>

          val idx = list.indexWhere{case (k, _) => ord.gt(k, sr.lastKey)}
          if(idx > 0) list = list.slice(0, idx)

          removeFromLeaf(sr.lastKey, sr.rctx, sr.lastVersion, list, removalVersion)
      }.flatMap { r =>
        pos += r.n

        //ctx.num_elements += n
        remove()
      }
    }

    remove().map { n =>
      RemovalResult(true, n)
    }.recover {
      case t: IndexError => RemovalResult(false, 0, Some(t))
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
        case cmd: Remove[K, V] => remove(cmd.keys, cmd.version.getOrElse(version))
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
