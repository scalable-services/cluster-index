package services.scalable.index.cluster

import com.google.protobuf.ByteString
import services.scalable.index.Commands.{Insert, Remove, Update}
import services.scalable.index.Errors.IndexError
import services.scalable.index.cluster.grpc.KeyIndexContext
import services.scalable.index.grpc.IndexContext
import services.scalable.index.{BatchResult, Commands, Errors, IndexBuilt, InsertionResult, QueryableIndex, RemovalResult, UpdateResult}

import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class ClusterIndex[K, V](val metaContext: IndexContext)
                        (implicit val rangeBuilder: IndexBuilt[K, V],
                         val clusterBuilder: IndexBuilt[K, KeyIndexContext]){

  import rangeBuilder._

  //private val disposable = new AtomicBoolean(false)

  val meta = new QueryableIndex[K, KeyIndexContext](metaContext)(clusterBuilder)
  val ranges = TrieMap.empty[String, RangeIndex[K, V]]

  def save(): Future[IndexContext] = {
    //assert(!disposable.get(), s"The cluster index ${metaContext.id} was already saved! Create another instance to perform new operations!")

    //disposable.set(true)

    saveIndexes().flatMap { ok =>
      meta.save()
    }
  }

  def saveIndexes(): Future[Boolean] = {
    Future.sequence(ranges.map { case (id, range) =>
      println(s"saving range[2] ${id} => ${range.currentSnapshot().id}")

      storage.loadOrCreate(range.snapshot()).flatMap { ok =>
        range.save()
      }

    }).map(_.toSeq.length == ranges.size)
  }

  def findPath(k: K): Future[Option[(K, RangeIndex[K, V], String)]] = {
    meta.findPath(k).flatMap {
      case None => Future.successful(None)
      case Some(leaf) =>
        val (_, (key, kctx, vs)) = leaf.findPath(k)

        if (!ranges.isDefinedAt(kctx.rangeId)) {
          println(s"${Console.YELLOW_B}RANGE NOT FOUND...${Console.RESET}")
        }

        ranges.get(kctx.rangeId) match {

          case None => RangeIndex.fromId[K, V](kctx.rangeId)(rangeBuilder).map { range =>
            ranges.put(kctx.rangeId, range)
            Some(key, range, vs)
          }

          case Some(range) => Future.successful(Some(key, range, vs))
        }
    }
  }

  def insertMeta(left: RangeIndex[K, V], version: String): Future[BatchResult] = {
    println(s"insert indexes in meta[1]: left ${left.currentSnapshot().id}")

    left.max.map(_.get).flatMap { max =>
      meta.execute(Seq(Commands.Insert[K, KeyIndexContext](
        metaContext.id,
        Seq(Tuple3(max, KeyIndexContext(ByteString.copyFrom(rangeBuilder.keySerializer.serialize(max)),
          left.currentSnapshot().id, left.currentSnapshot().lastChangeVersion), false)),
        Some(version)
      )), version)
    }
  }

  def insertMeta(left: RangeIndex[K, V], right: RangeIndex[K, V], last: (K, Option[String]), version: String): Future[BatchResult] = {

    println(s"inserting indexes in meta[2]: left ${left.currentSnapshot().id} right: ${right.currentSnapshot().id}")

    Future.sequence(Seq(left.max.map(_.get), right.max.map(_.get))).flatMap { maxes =>
      val lm = maxes(0)
      val rm = maxes(1)

      meta.execute(Seq(
        Commands.Remove[K, KeyIndexContext](metaContext.id, Seq(last), Some(version)),
        Commands.Insert[K, KeyIndexContext](metaContext.id, Seq(
          Tuple3(lm, KeyIndexContext(ByteString.copyFrom(rangeBuilder.keySerializer.serialize(lm)),
            left.getId(), left.getLastChangeVersion()), false),
          Tuple3(rm, KeyIndexContext(ByteString.copyFrom(rangeBuilder.keySerializer.serialize(rm)),
            right.getId(), right.getLastChangeVersion()), false)
        ), Some(version))
      ), version)
    }
  }

  def removeFromMeta(last: (K, Option[String]), version: String): Future[BatchResult] = {
    println(s"removing from meta: ${last}...")
    //meta.remove(Seq(last))

    meta.execute(Seq(
      Commands.Remove[K, KeyIndexContext](metaContext.id, Seq(last), Some(version))
    ), version)
  }

  def insertRange(left: RangeIndex[K, V], list: Seq[Tuple3[K, V, Boolean]], last: (K, Option[String]),
                  version: String): Future[Int] = {

    val lindex = left.copy(true)

    ranges.put(lindex.getId(), lindex)

    val remaining = (lindex.getMaxElements() - lindex.getNumElements()).toInt
    val n = Math.min(remaining, list.length)
    val slice = list.slice(0, n)

    assert(remaining >= 0, s"remaining >= 0 but remaining = ${remaining} lindex MaxNItems: ${lindex.getMaxElements()} lindex size: ${lindex.getNumElements()}")

    if (remaining == 0) {
      return lindex.split().flatMap { rindex =>
        println(s"${Console.CYAN_B}splitting index ${lindex.currentSnapshot().id}... ${Console.RESET}")

        ranges.put(lindex.getId(), lindex)
        ranges.put(rindex.getId(), rindex)

        println(s"inserted index ${rindex.getId()} with left being: ${lindex.getId()}")
        println(s"ranges1: ${ranges.map(_._2.currentSnapshot().id)}")
        println(s"ranges2: ${ranges.map(_._2.currentSnapshot().id)}")

        insertMeta(lindex, rindex, last, version).map(_ => 0)
      }
    }

    println(s"insert normally ${slice.length} ", slice.map { x => rangeBuilder.ks(x._1) })

    //ranges.put(lindex.meta.id, lindex)

    lindex.execute(Seq(Commands.Insert[K, V](metaContext.id, slice, Some(version))), version)
      .flatMap { case result =>
        assert(result.success, result.error.get)

        // Update range index on disk
        ranges.put(lindex.getId(), lindex)

        lindex.max.map(_.get).flatMap { lm =>
          meta.execute(Seq(
            Commands.Remove[K, KeyIndexContext](metaContext.id, Seq(last), Some(version)),
            Commands.Insert[K, KeyIndexContext](metaContext.id, Seq(
              Tuple3(lm, KeyIndexContext(ByteString.copyFrom(rangeBuilder.keySerializer.serialize(lm)),
                lindex.getId(), lindex.getLastChangeVersion()), false)
            ), Some(version))
          ), version).map { _ => slice.length }
        }
      }
  }

  def insertEmpty(data: Seq[Tuple3[K, V, Boolean]], version: String): Future[Int] = {

    val leftN = Math.min(rangeBuilder.MAX_N_ITEMS, data.length)
    val slice = data.slice(0, leftN.toInt)

    val rangeMeta = IndexContext()
      .withId(UUID.randomUUID.toString)
      .withLastChangeVersion(UUID.randomUUID.toString)
      .withMaxNItems(rangeBuilder.MAX_N_ITEMS)
      .withLevels(0)
      .withNumElements(0L)
      .withNumLeafItems(rangeBuilder.MAX_LEAF_ITEMS)
      .withNumMetaItems(rangeBuilder.MAX_META_ITEMS)

    val range = new RangeIndex[K, V](rangeMeta)(rangeBuilder)

    ranges.put(range.currentSnapshot().id, range)

    println(s"inserted range ${slice.length} ${range.currentSnapshot().id}...")

    range.execute(Seq(Commands.Insert[K, V](rangeMeta.id, slice, Some(version))), version)
      .flatMap { result =>
        assert(result.success, result.error.get)
        insertMeta(range, version).map(_ => slice.length)
      }
  }

  def insert(data: Seq[Tuple3[K, V, Boolean]], version: String)(implicit ord: Ordering[K]): Future[InsertionResult] = {
    val sorted = data.sortBy(_._1)

    if (sorted.exists { case (k, _, _) => sorted.count { case (k1, _, _) => ord.equiv(k, k1) } > 1 }) {
      return Future.successful(InsertionResult(false, 0, Some(Errors.DUPLICATED_KEYS(data.map(_._1), ks))))
    }

    val len = sorted.length
    var pos = 0

    def insert(): Future[Int] = {
      if (pos == len) return Future.successful(sorted.length)

      var list = sorted.slice(pos, len)
      val (k, _, _) = list(0)

      findPath(k).flatMap {
        case None =>

          insertEmpty(list, version).map { n =>
            println("meta n: ", meta.ctx.num_elements)
            n
          }

        case Some((last, index, vs)) =>

          val idx = list.indexWhere { case (k, _, _) => ord.gt(k, last) }
          if (idx > 0) list = list.slice(0, idx)

          insertRange(index, list, (last, Some(vs)), version)
      }.flatMap { n =>
        println(s"\ninserted: ${n}\n")
        pos += n
        insert()
      }
    }

    insert().map { n =>
      InsertionResult(true, n)
    }.recover {
      case t: IndexError =>
        t.printStackTrace()
        InsertionResult(false, 0, Some(t))
      case t: Throwable =>
        t.printStackTrace()
        throw t
    }
  }

  def update(data: Seq[Tuple3[K, V, Option[String]]], version: String): Future[UpdateResult] = {

    val sorted = data.sortBy(_._1)

    if (sorted.exists { case (k, _, _) => sorted.count { case (k1, _, _) => ord.equiv(k, k1) } > 1 }) {
      return Future.successful(UpdateResult(false, 0, Some(Errors.DUPLICATED_KEYS(sorted.map(_._1), ks))))
    }

    val len = sorted.length
    var pos = 0

    def update(): Future[Int] = {
      if (len == pos) return Future.successful(sorted.length)

      var list = sorted.slice(pos, len)
      val (k, _, _) = list(0)

      findPath(k).flatMap {
        case None => Future.failed(Errors.KEY_NOT_FOUND(k, ks))
        case Some((last, range, vs)) =>

          val idx = list.indexWhere { case (k, _, _) => ord.gt(k, last) }
          if (idx > 0) list = list.slice(0, idx)

          val copy = range.copy(true)
          ranges.put(copy.getId(), copy)

          copy.execute(Seq(Commands.Update[K, V](metaContext.id, list, Some(version))), version).map { result =>
            assert(result.success, result.error.get)

            (result, copy, list.length)
          }
      }.flatMap { case (res, copy, n) =>
        if (!res.success) {
          throw res.error.get
        } else {

          ranges.update(copy.currentSnapshot().id, copy)

          pos += n
          update()
        }
      }
    }

    update().map { n =>
      UpdateResult(true, n)
    }.recover {
      case t: IndexError =>
        t.printStackTrace()
        UpdateResult(false, 0, Some(t))
      case t: Throwable =>
        t.printStackTrace()
        throw t
    }
  }

  def borrow(range: RangeIndex[K, V], last: K, rangeVersion: String, txVersion: String): Future[Boolean] = {

    def updateMeta(borrowingRange: RangeIndex[K, V], borrowingMaxKey: K, borrowingVs: String,
                   range: RangeIndex[K, V], last: K, version: String): Future[Boolean] = {
      for {
        maxNext <- borrowingRange.max().map(_.get)
        maxTarget <- range.max().map(_.get)
        r <- {
          meta.execute(Seq(
            Commands.Remove[K, KeyIndexContext](metaContext.id, Seq(
              last -> Some(version),
              borrowingMaxKey -> Some(borrowingVs)
            ), Some(txVersion)),

            Commands.Insert(metaContext.id, Seq(
              (maxNext,  KeyIndexContext(ByteString.copyFrom(rangeBuilder.keySerializer.serialize(maxNext)),
                borrowingRange.currentSnapshot().id, borrowingRange.currentSnapshot().lastChangeVersion), false),

              (maxTarget, KeyIndexContext(ByteString.copyFrom(rangeBuilder.keySerializer.serialize(maxTarget)),
                range.currentSnapshot().id, range.currentSnapshot().lastChangeVersion), false)
            ), Some(txVersion))

          ), txVersion).map { rs =>

            ranges.put(borrowingRange.getId(), borrowingRange)
            ranges.put(range.getId(), range)

            rs.success
          }
        }
      } yield {
        r
      }
    }

    def merge(leftRange: Option[(RangeIndex[K, V], K, String)],
              rightRange: Option[(RangeIndex[K, V], K, String)]): Future[Boolean] = {
      (leftRange, rightRange) match {
        case (None, Some((rightR, nextK, nextVs))) => range.merge(rightR, txVersion).flatMap { merged =>
          merged.max().map(_.get).flatMap { maxMerged =>
            meta.execute(Seq(
              Commands.Remove[K, KeyIndexContext](metaContext.id, Seq(
                last -> Some(rangeVersion),
                nextK -> Some(nextVs)
              ), Some(txVersion)),

              Commands.Insert(metaContext.id, Seq(
                (maxMerged, KeyIndexContext(ByteString.copyFrom(rangeBuilder.keySerializer.serialize(maxMerged)),
                  merged.currentSnapshot().id, merged.currentSnapshot().lastChangeVersion), false)
              ), Some(txVersion))

            ), txVersion).map { rs =>

              ranges.remove(rightR.getId())
              ranges.remove(range.getId())
              ranges.put(merged.getId(), merged)

              rs.success
            }
          }
        }
        case (Some((leftR, prevK, prevVs)), None) => leftR.merge(range, txVersion).flatMap { merged =>
          merged.max().map(_.get).flatMap { maxMerged =>
            meta.execute(Seq(
              Commands.Remove[K, KeyIndexContext](metaContext.id, Seq(
                last -> Some(rangeVersion),
                prevK -> Some(prevVs)
              ), Some(txVersion)),

              Commands.Insert(metaContext.id, Seq(
                (maxMerged, KeyIndexContext(ByteString.copyFrom(rangeBuilder.keySerializer.serialize(maxMerged)),
                  merged.currentSnapshot().id, merged.currentSnapshot().lastChangeVersion), false)
              ), Some(txVersion))

            ), txVersion).map { rs =>

              ranges.remove(leftR.getId())
              ranges.remove(range.getId())
              ranges.put(merged.getId(), merged)

              rs.success
            }
          }
        }
        case (Some((leftR, prevK, prevVs)), Some((rightR, nextK, nextVs))) => leftR.merge(range, txVersion).flatMap { merged =>
          merged.max().map(_.get).flatMap { maxMerged =>
            meta.execute(Seq(
              Commands.Remove[K, KeyIndexContext](metaContext.id, Seq(
                last -> Some(rangeVersion),
                prevK -> Some(prevVs)
              ), Some(txVersion)),

              Commands.Insert(metaContext.id, Seq(
                (maxMerged, KeyIndexContext(ByteString.copyFrom(rangeBuilder.keySerializer.serialize(maxMerged)),
                  merged.currentSnapshot().id, merged.currentSnapshot().lastChangeVersion), false)
              ), Some(txVersion))

            ), txVersion).map { rs =>

              ranges.remove(leftR.getId())
              ranges.remove(range.getId())
              ranges.put(merged.getId(), merged)

              rs.success
            }
          }
        }

        case (None, None) =>
          range.max().flatMap {
            case None => meta.execute(Seq(
              Commands.Remove[K, KeyIndexContext](metaContext.id, Seq(
                last -> Some(rangeVersion)
              ), Some(txVersion))
            ), txVersion).map { rs =>

              ranges.remove(range.getId(), range)

              rs.success
            }

            case Some(maxTarget) => meta.execute(Seq(
              Commands.Remove[K, KeyIndexContext](metaContext.id, Seq(
                last -> Some(rangeVersion)
              ), Some(txVersion)),

              Commands.Insert(metaContext.id, Seq(
                (maxTarget, KeyIndexContext(ByteString.copyFrom(rangeBuilder.keySerializer.serialize(maxTarget)),
                  range.currentSnapshot().id, range.currentSnapshot().lastChangeVersion), false)
              ), Some(txVersion))

            ), txVersion).map { rs =>

              ranges.put(range.getId(), range)

              rs.success
            }
        }
      }
    }

    def borrowFromRight(rightRange: RangeIndex[K, V], nextK: K, nextVs: String,
                        leftRange: Option[(RangeIndex[K, V], K, String)]): Future[Boolean] = {
      if(!rightRange.hasEnough()) return merge(leftRange, Some(rightRange, nextK, nextVs))

      rightRange.borrowRight(range).map(_.get).flatMap { range =>
        updateMeta(rightRange, nextK, nextVs, range, last, rangeVersion)
      }
    }

    def borrowFromRight2(leftRange: Option[(RangeIndex[K, V], K, String)]): Future[Boolean] = {
      meta.nextKey(last)(ord).flatMap {
        case None => merge(leftRange, None)
        case Some((nextK, nextCtx, nextVs)) =>
          val leftRangeOpt = ranges.get(nextCtx.rangeId)

          if(leftRangeOpt.isDefined){
            borrowFromRight(leftRangeOpt.get, nextK, nextVs, leftRange)
          } else {
            RangeIndex.fromId[K, V](nextCtx.rangeId)(rangeBuilder)
              .flatMap(x => borrowFromRight(x, nextK, nextVs, leftRange))
          }
      }
    }

    def borrowFromLeft(leftRange: RangeIndex[K, V], prevK: K, prevVs: String): Future[Boolean] = {
      if(!leftRange.hasEnough()) return borrowFromRight2(Some(leftRange, prevK, prevVs))

      leftRange.borrowLeft(range).map(_.get).flatMap { range =>
        updateMeta(leftRange, prevK, prevVs, range, last, rangeVersion)
      }
    }

    meta.previousKey(last)(ord).flatMap {
      case None => borrowFromRight2(None)
      case Some((prevK, prevCtx, prevVs)) =>
        val leftRangeOpt = ranges.get(prevCtx.rangeId)

        if(leftRangeOpt.isDefined){
          borrowFromLeft(leftRangeOpt.get, prevK, prevVs)
        } else {
          RangeIndex.fromId[K, V](prevCtx.rangeId)(rangeBuilder)
            .flatMap(x => borrowFromLeft(x, prevK, prevVs))
        }
    }
  }

  def remove(data: Seq[Tuple2[K, Option[String]]], version: String): Future[RemovalResult] = {

    val sorted = data.sortBy(_._1)

    if (sorted.exists { case (k, _) => sorted.count { case (k1, _) => ord.equiv(k, k1) } > 1 }) {
      return Future.successful(RemovalResult(false, 0, Some(Errors.DUPLICATED_KEYS(sorted.map(_._1), ks))))
    }

    val len = sorted.length
    var pos = 0

    def remove(): Future[Int] = {
      if (len == pos) return Future.successful(sorted.length)

      var list = sorted.slice(pos, len)
      val (k, _) = list(0)

      findPath(k).flatMap {
        case None => Future.failed(Errors.KEY_NOT_FOUND(k, ks))
        case Some((last, range, vs)) =>

          val idx = list.indexWhere { case (k, _) => ord.gt(k, last) }
          if (idx > 0) list = list.slice(0, idx)

          val rangeData = Await.result(range.inOrder(), Duration.Inf)
          val copy = range.copy(true)

          val maxNow = Await.result(range.max.map(_.get), Duration.Inf)

          val eq = ord.equiv(maxNow, last)

          val rangeDataCopy = Await.result(range.inOrder(), Duration.Inf)

          assert(rangeData.map(_._1) == rangeDataCopy.map(_._1))

          val notmissingCopy = list.filterNot { case (k, _) =>
            rangeData.exists{case (k1, _, _) => ord.equiv(k, k1)}
          }

          val all = inOrder()
          val missingInAll = notmissingCopy.filterNot { case (k, _) =>
            all.exists{case (k1, _, _) => ord.equiv(k, k1)}
          }

          copy.execute(Seq(Commands.Remove[K, V](metaContext.id, list)), version).flatMap { result =>
            rangeData
            notmissingCopy
            rangeDataCopy
            missingInAll
            eq
              println("eq = ", eq)
              println(result)

              if(!result.success)
              {
                println()
              }

              assert(result.success, result.error.get)

              if(copy.hasMinimum()) {
                Future.successful(Tuple3(result, copy, list.length))
              } else {
                borrow(copy, last, vs, version).map(_ => Tuple3(result, copy, list.length))
              }
            }

      }.flatMap { case (res, copy, n) =>

        if (!res.success) {
          throw res.error.get
        } else {
          //ranges.put(copy.index.ctx.currentSnapshot().id, copy)

          pos += n
          remove()
        }
      }
    }

    remove().map { n =>
      RemovalResult(true, n)
    }.recover {
      case t: IndexError =>
        t.printStackTrace()
        RemovalResult(false, 0, Some(t))
      case t: Throwable =>
        t.printStackTrace()
        throw t
    }
  }

  def execute(cmds: Seq[Commands.Command[K, V]], version: String): Future[BatchResult] = {

    def process(pos: Int, error: Option[Throwable]): Future[BatchResult] = {
      if (error.isDefined) {
        return Future.successful(BatchResult(false, error))
      }

      if (pos == cmds.length) {
        return Future.successful(BatchResult(true))
      }

      val cmd = cmds(pos)

      (cmd match {
        case cmd: Insert[K, V] => insert(cmd.list, version)
        case cmd: Update[K, V] => update(cmd.list, version)
        case cmd: Remove[K, V] => remove(cmd.keys, version)
      }).flatMap(prev => process(pos + 1, prev.error))
    }

    process(0, None)
  }

  def inOrder(): Seq[(K, V, String)] = {
    val iter = Await.result(meta.all(), Duration.Inf)

    println(s"${Console.CYAN_B}meta keys: ${iter.map(x => rangeBuilder.ks(x._1))}${Console.RESET}")

    iter.map { case (k, link, version) =>
      val range = ranges.get(link.rangeId) match {
        case None =>

          println(s"try to load the range: ${link.rangeId}...")

          val ctx = Await.result(storage.loadIndex(link.rangeId), Duration.Inf).get
          new RangeIndex[K, V](ctx)(rangeBuilder)

        case Some(range) => range
      }

      val list = Await.result(range.inOrder(), Duration.Inf)

      println(s"range id ${range.getId()} hasminimum: ${range.hasMinimum()} minimum: ${range.getMaxElements()/2} len: ${range.getNumElements()} [${k}] => ${list.map(_._1)}")

      list
    }.flatten
  }

}
