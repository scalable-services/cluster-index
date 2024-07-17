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
import scala.concurrent.duration.{DAYS, Duration}
import scala.concurrent.{Await, Future}

final class ClusterIndex[K, V](val descriptor: IndexContext)
                              (implicit val rangeBuilder: IndexBuilt[K, V],
                               testData: Seq[K]) {

  implicit val clusterBuilder = IndexBuilder
    .create[K, KeyIndexContext](rangeBuilder.ec, rangeBuilder.ord,
      descriptor.numLeafItems, descriptor.numMetaItems, descriptor.maxNItems,
      rangeBuilder.keySerializer, ClusterSerializers.keyIndexSerializer)
    .storage(rangeBuilder.storage)
    // .cache(rangeBuilder.cache)
    .build()

  import clusterBuilder._
  //import rangeBuilder._

  var testData1 = testData

  def checkPartialData(from: String): Boolean = {
    val ordered = inOrder().map(_._1)
    val testData2 = testData1.sorted

    println("index: ", ordered.map(x => rangeBuilder.ks.apply(x)))
    println("data: ", testData2.map(x => rangeBuilder.ks.apply(x)))

    val ok = ordered.length == testData2.length &&
      ordered.zipWithIndex.forall{case (x, i) => rangeBuilder.ord.equiv(x, testData2(i))}

    if(!ok){
      println()
    }

    println(s"${Console.GREEN_B}ok from ${from}...${Console.RESET}")

    ok
  }

  val meta = new QueryableIndex[K, KeyIndexContext](descriptor)(clusterBuilder)
  val ranges = TrieMap.empty[String, Range[K, V]]
  val toRemove = TrieMap.empty[String, String]

  def save(): Future[IndexContext] = {
    Future.sequence(ranges.map(_._2.save())).flatMap { allOk =>
      meta.save()
    }
  }

  def getRange(id: String): Future[Range[K, V]] = {
    ranges.get(id) match {
      case None => clusterBuilder.storage.loadIndex(id)
        .map{ctx => new Range[K, V](ctx.get)(rangeBuilder)}
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

        /*range.insert(data, insertVersion).map { r =>
          ranges.update(range.ctx.indexId, range)
          r.n
        }*/

        range.execute(Seq(Commands.Insert(kctx.rangeId, data, Some(insertVersion))))
          .map { r =>
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
        getRange(kctx.rangeId).map{r =>

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
        getRange(kctx.rangeId).map{r =>

          Some((r, kprev, kctx, lastK))
        }

      /*storage.loadIndex(kctx.rangeId)
        .map(_.get).map { c =>
          Some((new Range[K, V](c)(rangeBuilder), kprev, kctx, lastK))
        }*/
    }
  }

  protected def merge(leftInfo: (Range[K, V], K, KeyIndexContext, String),
                      rightInfo: (Range[K, V], K, KeyIndexContext, String), version: String, keys: Seq[K]): Future[Int] = {
    val (left, leftLastKey, leftCtx, leftLastVersion) = leftInfo
    val (right, rightLastKey, rightCtx, rightLastVersion) = rightInfo

    left.merge(right, version).flatMap{ merged => merged.max().map(_.get).map(merged -> _)}
      .flatMap { case (merged, (mergedLastKey, _, _)) =>

      val mergedCtx = KeyIndexContext()
        .withRangeId(merged.ctx.indexId)
        .withLastChangeVersion(merged.ctx.lastChangeVersion)
        .withKey(ByteString.copyFrom(rangeBuilder.keySerializer.serialize(mergedLastKey)))

      meta.execute(Seq(
        Commands.Remove(leftCtx.rangeId, Seq(
          leftLastKey -> Some(leftLastVersion),
          rightLastKey -> Some(rightLastVersion)
        ), Some(version)),

        Commands.Insert(merged.ctx.indexId, Seq(
          (mergedLastKey, mergedCtx, true)
        ), Some(version))
      )).map { _ =>

        ranges.remove(left.ctx.indexId)
        ranges.remove(right.ctx.indexId)
        ranges.put(merged.ctx.indexId, merged)

        if(!checkPartialData("merge")){

          val yy = testData1
          val xx = inOrder()

          println()
          assert(false)
        }

        0
      }
    }
  }
  
  protected def borrow(rangeInfo: (Range[K, V], K, KeyIndexContext, String),
                       auxInfo: (Range[K, V], K, KeyIndexContext, String), version: String): Future[Int] = {

    val (range, rangeLastKey, rangeCtx, rangeLastVersion) = rangeInfo
    val (aux, auxLastKey, auxCtx, auxLastVersion) = auxInfo

    aux.borrow(range, version).flatMap { borrower =>

      range.max().map(_.get).flatMap { case (rangeMax, _, _) =>
        borrower.max().map(_.get).flatMap { case (borrowerMax, _, _) =>

          val targetCtx = KeyIndexContext()
            .withRangeId(range.ctx.indexId)
            .withLastChangeVersion(range.ctx.lastChangeVersion)
            .withKey(ByteString.copyFrom(rangeBuilder.keySerializer.serialize(rangeMax)))

          val borrowerCtx = KeyIndexContext()
            .withRangeId(borrower.ctx.indexId)
            .withLastChangeVersion(borrower.ctx.lastChangeVersion)
            .withKey(ByteString.copyFrom(rangeBuilder.keySerializer.serialize(borrowerMax)))

          meta.execute(Seq(
            Commands.Remove(range.ctx.indexId, Seq(
              rangeLastKey -> Some(rangeLastVersion),
              auxLastKey -> Some(auxLastVersion)
            ), Some(version)),

            Commands.Insert(range.ctx.indexId, Seq(
              (rangeMax, targetCtx, true),
              (borrowerMax, borrowerCtx, true)
            ), Some(version))
          )).map { _ =>

            ranges.put(range.ctx.indexId, range)
            ranges.put(borrower.ctx.indexId, borrower)

            if(!checkPartialData("borrowing")) {
              println()
              assert(false)
            }

            0
          }
        }
      }

    }
  }

  protected def whoCanBorrow(target: Range[K, V],
                             leftInfo: Option[(Range[K, V], K, KeyIndexContext, String)],
                             rightInfo: Option[(Range[K, V], K, KeyIndexContext, String)]): Future[Option[(Range[K, V], K, KeyIndexContext, String)]] = {
    (leftInfo, rightInfo) match {
      case (None, Some(right)) => right._1.canBorrow(target.missingToMin()).map {
        case true => rightInfo
        case false => None
      }

      case (Some(left), None) => left._1.canBorrow(target.missingToMin()).map {
        case true => leftInfo
        case false => None
      }

      case (Some(left), Some(right)) => left._1.canBorrow(target.missingToMin()).flatMap {
        case true => Future.successful(leftInfo)
        case false => right._1.canBorrow(target.missingToMin()).map {
          case true => rightInfo
          case false => None
        }
      }

      case (None, None) => Future.successful(None)
    }
  }

  protected def whoCanMerge(leftInfo: Option[(Range[K, V], K, KeyIndexContext, String)],
                             rightInfo: Option[(Range[K, V], K, KeyIndexContext, String)]): Future[Option[(Range[K, V], K, KeyIndexContext, String)]] = {
    (leftInfo, rightInfo) match {
      case (Some(left), None) => Future.successful(leftInfo)
      case (None, Some(right)) => Future.successful(rightInfo)
      case (Some(left), Some(right)) =>
        Future.successful(if(left._1.ctx.num_elements < right._1.ctx.num_elements) leftInfo else rightInfo)
      case (None, None) => Future.successful(None)
    }
  }

  protected def whoCanMerge2(leftInfo: Option[(Range[K, V], K, KeyIndexContext, String)],
                            rightInfo: Option[(Range[K, V], K, KeyIndexContext, String)]): Future[Option[(Range[K, V], K, KeyIndexContext, String)]] = {
    (leftInfo, rightInfo) match {
      case (Some(left), None) => Future.successful(if(left._1.ctx.num_elements < rangeBuilder.MAX_N_ITEMS/2) leftInfo else None)
      case (None, Some(right)) => Future.successful(if(right._1.ctx.num_elements < rangeBuilder.MAX_N_ITEMS/2) rightInfo else None)
      case (Some(left), Some(right)) =>

        if(left._1.ctx.num_elements < rangeBuilder.MAX_N_ITEMS/2){
          Future.successful(leftInfo)
        } else if(right._1.ctx.num_elements < rangeBuilder.MAX_N_ITEMS/2){
          Future.successful(rightInfo)
        } else {
          Future.successful(None)
        }

        //Future.successful(if(left._1.ctx.num_elements < right._1.ctx.num_elements) leftInfo else rightInfo)
      case (None, None) => Future.successful(None)
    }
  }

  protected def handleSingleRange(rangeInfo: (Range[K, V], K, KeyIndexContext, String), version: String): Future[Int] = {
    val (range, rangeLastKey, rangeCtx, rangeLastVersion) = rangeInfo

    range.isEmpty().flatMap {
      case true => meta.execute(Seq(
        Commands.Remove(range.ctx.indexId, Seq(
          rangeLastKey -> Some(rangeLastVersion)
        ), Some(version))
      )).map { _ =>
        ranges.remove(range.ctx.indexId)

        if(!checkPartialData("single empty")){
          println()
          assert(false)
        }

        0
      }

      case false =>
        ranges.update(range.ctx.indexId, range)

        if(!checkPartialData("single")){
          println()
          assert(false)
        }

        Future.successful(0)
    }
  }

  protected def tryToBorrow(range: Range[K, V], rangeCtx: KeyIndexContext, lastKey: K, lastVersion: String,
                       version: String, keys: Seq[K]): Future[Int] = {
    val rangeInfo = (range, lastKey, rangeCtx, lastVersion)

    (for {
      leftInfo <- getLeftRange(lastKey)
      rightInfo <- getRightRange(lastKey)

      borrowerInfo <- whoCanBorrow(range, leftInfo, rightInfo)
    } yield {
      (borrowerInfo, (leftInfo, rightInfo))
    }).flatMap {
      case (Some(borrowerInfo), _) => borrow(rangeInfo, borrowerInfo, version)
      case (None, (leftInfo, rightInfo)) => whoCanMerge(leftInfo, rightInfo).flatMap {
        case Some(mergerInfo) => {
          if(leftInfo.isDefined && leftInfo.get._1.ctx.indexId == mergerInfo._1.ctx.indexId){
            merge(leftInfo.get, rangeInfo, version, keys)
          } else {
            merge(rangeInfo, rightInfo.get, version, keys)
          }
        }
        case None => handleSingleRange(rangeInfo, version)
      }
    }

    /*(for {
      leftInfo <- getLeftRange(lastKey)
      rightInfo <- getRightRange(lastKey)

      mergerInfo <- whoCanMerge2(leftInfo, rightInfo)
    } yield {
      (mergerInfo, (leftInfo, rightInfo))
    }).flatMap {
      case (None, _) => handleSingleRange(rangeInfo, version)
      case (Some(merger), (leftInfo, rightInfo)) =>

        if(leftInfo.isDefined && leftInfo.get._1.ctx.indexId == merger._1.ctx.indexId){
          merge(leftInfo.get, rangeInfo, version, keys)
        } else {
          merge(rangeInfo, rightInfo.get, version, keys)
        }
      }*/

  }

  protected def removeFromLeaf(lastKey: K, kctx: KeyIndexContext, lastVersion: String, keys: Seq[(K, Option[String])],
                               removalVersion: String): Future[Int] = {

    //checkPartialData()
    testData1 = testData1.filterNot{x => keys.exists{x2 => rangeBuilder.ord.equiv(x, x2._1)}}

    getRange(kctx.rangeId).flatMap { l =>
      l.copy(sameId = true)
    }.flatMap { range =>
      range.execute(Seq(Commands.Remove(kctx.rangeId, keys, Some(removalVersion)))).flatMap { r =>
         range.hasMinimum().flatMap {
           case true =>

             ranges.update(range.ctx.indexId, range)

             if(!checkPartialData("simple removal")) {
               println(this.inOrder().length)
               assert(false)
             }
             
             Future.successful(r.n)

           case false => tryToBorrow(range, kctx, lastKey, lastVersion, removalVersion, keys.map(_._1)).map(_ => keys.length)
         }
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
        case None => Future.failed(Errors.KEY_NOT_FOUND[K](k, rangeBuilder.ks))
        case Some((last, kctx, lastVersion)) =>

          val idx = list.indexWhere{case (k, _) => ord.gt(k, last)}
          if(idx > 0) list = list.slice(0, idx)

          removeFromLeaf(last, kctx, lastVersion, list, removalVersion)
      }.flatMap { n =>
        pos += n

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

  def inOrder(): Seq[Tuple[K, V]] = {
      Await.result(meta.all(
        meta.inOrder()
      ).map { all =>

        all.map { case (lastKey, kctx, lastVersion) =>
          val range = Await.result(getRange(kctx.rangeId), Duration.Inf)
          val list = Await.result(range.inOrder(), Duration.Inf)

          println(s"range id: ${range.ctx.indexId} | maxKey: ${rangeBuilder.ks(lastKey)} | len: ${range.ctx.num_elements} list len: ${list.length}")

          list
        }.flatten
    }, Duration.Inf)
  }

  def inOrder2(filterNot: Seq[K]): Seq[Tuple[K, V]] = {
    Await.result(meta.all(
      meta.inOrder()
    ).map { all =>

      all.filterNot{x => filterNot.exists(y => ord.equiv(x._1, y))}.map { case (lastKey, kctx, lastVersion) =>
        val range = Await.result(getRange(kctx.rangeId), Duration.Inf)
        val list = Await.result(range.inOrder(), Duration.Inf)

        println(s"range id: ${range.ctx.indexId} | maxKey: ${rangeBuilder.ks(lastKey)} | len: ${range.ctx.num_elements} list len: ${list.length}")

        list
      }.flatten
    }, Duration.Inf)
  }

}
