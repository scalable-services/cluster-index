package cluster

import cluster.grpc.KeyIndexContext
import com.google.protobuf.ByteString
import services.scalable.index.Commands.{Insert, Remove, Update}
import services.scalable.index.Errors.IndexError
import services.scalable.index.grpc.IndexContext
import services.scalable.index.{AsyncIndexIterator, BatchResult, Commands, Errors, IndexBuilder, InsertionResult, QueryableIndex, RemovalResult, Tuple, UpdateResult}

import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class ClusterIndex[K, V](val metaContext: IndexContext,
                         val maxNItems: Int,
                         val numLeafItems: Int,
                         val numMetaItems: Int)(val indexBuilder: IndexBuilder[K, V],
                          val clusterMetaBuilder: IndexBuilder[K, KeyIndexContext]) {
  import clusterMetaBuilder._

  val meta = new QueryableIndex[K, KeyIndexContext](metaContext)(clusterMetaBuilder)
  var indexes = TrieMap.empty[String, QueryableIndex[K, V]]

  def save(clear: Boolean = true): Future[IndexContext] = {

    saveIndexes().flatMap { ok =>
      //indexes.clear()
      meta.save()
    }
  }

  def saveIndexes(clear: Boolean = true): Future[Boolean] = {
    Future.sequence(indexes.map { case (id, index) =>
      println(s"saving index[2] ${index.ctx.indexId} => ${index.ctx.currentSnapshot()}")

      TestHelper.loadOrCreateIndex(index.ctx.snapshot()).flatMap { _ =>
        index.save()
      }

    }).map(_.toSeq.length == indexes.size)
  }

  def findPath(k: K): Future[Option[(K, QueryableIndex[K, V], String)]] = {
    meta.findPath(k).map {
      case None => None
      case Some(leaf) =>
        val (_, (key, kctx, vs)) = leaf.findPath(k)

       // val idxs = this.indexes
       // val idx = indexes(ictx.id)

        if(!indexes.isDefinedAt(kctx.indexId)){
          println(s"${Console.RED_B}INDEX NOT DEFINED...${Console.RESET}")
        }

        val index = indexes.get(kctx.indexId)
          .getOrElse {
            val index = new QueryableIndex[K, V](Await.result(storage.loadIndex(kctx.indexId), Duration.Inf).get)(indexBuilder)

            indexes.put(kctx.indexId, index)

            index
          }

        Some(key, index, vs)
    }
  }

  def insertMeta(left: QueryableIndex[K, V]): Future[InsertionResult] = {
    println(s"insert indexes in meta[1]: left ${left.ctx.indexId}")

    left.max().flatMap { lm =>
      meta.insert(Seq(Tuple3(lm.get._1, KeyIndexContext(ByteString.copyFrom(indexBuilder.keySerializer.serialize(lm.get._1)),
        left.ctx.indexId, left.ctx.id), true)))
    }
  }

  def insertMeta(left: QueryableIndex[K, V], right: QueryableIndex[K, V], last: (K, Option[String])): Future[InsertionResult] = {
    Future.sequence(Seq(left.max(), right.max())).flatMap { maxes =>
      val lm = maxes(0).get._1
      val rm = maxes(1).get._1

      println(s"inserting indexes in meta[2]: left ${left.ctx.indexId} right: ${right.ctx.indexId}")

      meta.remove(Seq(last)).flatMap { ok =>
        meta.insert(Seq(
          Tuple3(lm, KeyIndexContext(ByteString.copyFrom(indexBuilder.keySerializer.serialize(lm)),
            left.ctx.indexId, left.ctx.id), true),
          Tuple3(rm, KeyIndexContext(ByteString.copyFrom(indexBuilder.keySerializer.serialize(rm)),
            right.ctx.indexId, right.ctx.id), true)
        ))
      }
    }
  }

  def removeFromMeta(left: QueryableIndex[K, V], last: (K, Option[String])): Future[RemovalResult] = {
    println(s"removing from meta: ${last}...")

    meta.remove(Seq(last)).map { res =>
      println("ok")
      res
    }
  }

  def insertEmpty(data: Seq[Tuple3[K, V, Boolean]]): Future[Int] = {
    val leftN = Math.min(maxNItems, data.length)
    val slice = data.slice(0, leftN)

    val index = new QueryableIndex[K, V](
      IndexContext()
      .withId(UUID.randomUUID().toString)
      .withLastChangeVersion(UUID.randomUUID.toString)
      .withMaxNItems(maxNItems)
      .withNumElements(0)
      .withLevels(0)
      .withNumLeafItems(numLeafItems)
      .withNumMetaItems(numMetaItems))(indexBuilder)

    indexes.put(index.ctx.indexId, index)

    println(s"inserted index ${index.ctx.indexId}")

    index.insert(slice).flatMap { _ =>
      //println()
      insertMeta(index).map(_ => slice.length)
    }
  }

  def insertRange(left: QueryableIndex[K, V], list: Seq[Tuple3[K, V, Boolean]], last: (K, Option[String])): Future[Int] = {

    val lindex = left.copy()

    val remaining = lindex.ctx.maxNItems - lindex.ctx.num_elements
    val n = Math.min(remaining, list.length).toInt
    val slice = list.slice(0, n)

    if(remaining == 0){
      return for {
        rindex <- lindex.split().map { rindex =>

          println(s"${Console.CYAN_B}splitting index ${lindex.ctx.indexId}... ${Console.RESET}")

          indexes.put(lindex.ctx.indexId, lindex)

          println(s"inserted index ${rindex.ctx.id} with left being: ${lindex.ctx.indexId}")

          indexes.put(rindex.ctx.indexId, rindex)
          rindex
        }

        n <- insertMeta (lindex, rindex, last).map { n =>
          0
        }

      } yield {
        n
      }
    }

    println(s"insert normally ", slice.map{x => indexBuilder.ks(x._1)})

    /*val lindex = new QueryableIndex[K, V](IndexContext(left.ctx.id,
      left.ctx.NUM_LEAF_ENTRIES, left.ctx.NUM_META_ENTRIES, left.ctx.root.map { r => RootRef(r._1, r._2) },
      left.ctx.levels,
      left.ctx.num_elements, left.ctx.maxNItems))

    lindex.ctx.blockReferences ++= refs

    indexes.put(left.ctx.indexId, lindex)*/

    indexes.put(lindex.ctx.indexId, lindex)

    lindex.insert(slice).flatMap { _ =>
      Future.sequence(Seq(lindex.max())).flatMap { maxes =>
        val lm = maxes(0).get._1

        val hasChanged = !indexBuilder.ord.equiv(lm, last._1)

        if(hasChanged){

          println(s"${Console.CYAN_B}max key has changed for index ${lindex.ctx.indexId}... from ${indexBuilder.ks(last._1)} to ${indexBuilder.ks(lm)} ${Console.RESET}")

          meta.remove(Seq(last)).flatMap { ok =>
            meta.insert(Seq(
              Tuple3(lm, KeyIndexContext(ByteString.copyFrom(indexBuilder.keySerializer.serialize(lm)),
                lindex.ctx.indexId, lindex.ctx.id), true)
            ))
          }
        } else {
          Future.successful(InsertionResult(true, 0, None))
        }

      }.map(_ => slice.length)
    }
  }

  def insert(data: Seq[Tuple3[K, V, Boolean]])(implicit ord: Ordering[K]): Future[InsertionResult] = {
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

          insertEmpty(list).map { n =>
            println("meta n: ", meta.ctx.num_elements)
            n
          }
        case Some((last, index, vs)) =>

          val idx = list.indexWhere { case (k, _, _) => ord.gt(k, last) }
          if (idx > 0) list = list.slice(0, idx)

          insertRange(index, list, (last, Some(vs)))
      }.flatMap { n =>
        println(s"\ninserted: ${n}\n")
        pos += n
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

  def update(data: Seq[Tuple3[K, V, Option[String]]])(implicit ord: Ordering[K]): Future[UpdateResult] = {

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
        case Some((last, index, vs)) =>

          val idx = list.indexWhere { case (k, _, _) => ord.gt(k, last) }
          if (idx > 0) list = list.slice(0, idx)

          index.update(list)
      }.flatMap { res =>

        if(!res.success) {
          throw res.error.get
        } else {
          pos += res.n
          update()
        }
      }
    }

    update().map { n =>
      UpdateResult(true, n)
    }.recover {
      case t: IndexError => UpdateResult(false, 0, Some(t))
      case t: Throwable => throw t
    }
  }

  def remove(data: Seq[Tuple2[K, Option[String]]])(implicit ord: Ordering[K]): Future[RemovalResult] = {

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
        case Some((last, index, vs)) =>

          val idx = list.indexWhere { case (k, _) => ord.gt(k, last) }
          if (idx > 0) list = list.slice(0, idx)

          index.remove(list).flatMap { res =>

            if (!res.success) {
              throw res.error.get
            } else if (index.isEmpty()) {

              removeFromMeta(index, (last, Some(vs))).map { res1 =>

                if (!res1.success) {
                  throw res1.error.get
                } else {

                  println(s"${Console.CYAN_B}index ${index.ctx.indexId} is now empty! ${Console.RESET}")
                  index.ctx.lastChangeVersion = UUID.randomUUID.toString

                  res
                }

              }

            } else {

              index.max().map { max =>
                val hasChanged = !indexBuilder.ord.equiv(max.get._1, last)

                if(hasChanged){
                  println(s"${Console.CYAN_B}max key has changed: ${index.ctx.indexId} from ${indexBuilder.ks(last)} to ${indexBuilder.ks(max.get._1)}${Console.RESET}")
                  index.ctx.lastChangeVersion = UUID.randomUUID.toString
                }

                res
              }

            }

          }
      }.flatMap { res =>

        if (!res.success) {
          throw res.error.get
        } else {
          pos += res.n
          remove()
        }
      }
    }

    remove().map { n =>
      RemovalResult(true, n)
    }.recover {
      case t: IndexError => RemovalResult(false, 0, Some(t))
      case t: Throwable => throw t
    }
  }

  def execute(cmds: Seq[Commands.Command[K, V]]): Future[BatchResult] = {

    def process(pos: Int, error: Option[Throwable]): Future[BatchResult] = {
      if (error.isDefined) {
        return Future.successful(BatchResult(false, error))
      }

      if (pos == cmds.length) {
        return Future.successful(BatchResult(true))
      }

      val cmd = cmds(pos)

      (cmd match {
        case cmd: Insert[K, V] => insert(cmd.list)
        case cmd: Update[K, V] => update(cmd.list)
        case cmd: Remove[K, V] => remove(cmd.keys)
      }).flatMap(prev => process(pos + 1, prev.error))
    }

    process(0, None)
  }

  def all[K, V](it: AsyncIndexIterator[Seq[Tuple[K, V]]])(implicit ec: ExecutionContext): Future[Seq[Tuple[K, V]]] = {
    it.hasNext().flatMap {
      case true => it.next().flatMap { list =>
        all(it).map {
          list ++ _
        }
      }
      case false => Future.successful(Seq.empty[Tuple[K, V]])
    }
  }

  def inOrder(): Seq[(K, V, String)] = {
    val iter = Await.result(all(meta.inOrder()), Duration.Inf)

    println(s"${Console.CYAN_B}meta keys: ${iter.map(x => indexBuilder.ks(x._1))}${Console.RESET}")

    iter.map { case (k, link, version) =>
      val ctx = Await.result(storage.loadIndex(link.indexId), Duration.Inf).get

      //println(s"range n: ${ctx.numElements}")
      val index = new QueryableIndex[K, V](ctx)(indexBuilder)
      val r = Await.result(all(index.inOrder()), Duration.Inf)

      //println(s"index ${link.ctxId} link: ${ctx.root} inOrder: ${r.map{x => indexBuilder.ks(x._1) -> indexBuilder.vs(x._2)}}")

      r
    }.flatten
  }

  def inOrderFromSaved(): Seq[(K, V, String)] = {
    val iter = Await.result(all(meta.inOrder()), Duration.Inf)

    println(s"${Console.CYAN_B}meta keys: ${iter.map(x => indexBuilder.ks(x._1))}${Console.RESET}")

    iter.map { case (k, link, version) =>

      val ctx = Await.result(storage.loadIndex(link.indexId), Duration.Inf).get

      val link2 = Await.result(storage.loadIndex(ctx.id), Duration.Inf).get
      val r = Await.result(all(new QueryableIndex[K, V](link2)(indexBuilder).inOrder()), Duration.Inf)

      println(s"range ${ctx.id} n: ${ctx.numElements}")

      r
    }.flatten
  }
}

object ClusterIndex {

  def fromRange[K, V](rangeId: String,
                      numLeafItems: Int,
                      numMetaItems: Int)(indexBuilder: IndexBuilder[K, V],
                                         clusterMetaBuilder: IndexBuilder[K, KeyIndexContext]
  ): Future[(K, ClusterIndex[K, V])] = {

    import indexBuilder._

    val metaCtx = IndexContext()
      .withId(UUID.randomUUID.toString)
      .withMaxNItems(Int.MaxValue)
      .withLevels(0)
      .withNumLeafItems(numLeafItems)
      .withNumMetaItems(numMetaItems)
      .withLastChangeVersion(UUID.randomUUID.toString)

    val metaRange = new QueryableIndex[K, KeyIndexContext](metaCtx)(clusterMetaBuilder)

    for {
      rangeCtx <- storage.loadIndex(rangeId).map(_.get)

      rangeIndex = new QueryableIndex[K, V](rangeCtx)(indexBuilder)

      maxRangeK <- rangeIndex.max().map(_.get).map(_._1)

      n <- metaRange.insert(Seq(
        Tuple3(maxRangeK, KeyIndexContext(ByteString.copyFrom(indexBuilder.keySerializer.serialize(maxRangeK)), rangeCtx.id,
          rangeCtx.lastChangeVersion), true)
      ))

    } yield {

      val clusterRangeCtx = metaRange.ctx.snapshot()

      val metaCR = new ClusterIndex[K, V](clusterRangeCtx, clusterRangeCtx.maxNItems,
        clusterRangeCtx.numLeafItems, clusterRangeCtx.numMetaItems)(indexBuilder, clusterMetaBuilder)

      metaCR.indexes.put(rangeIndex.ctx.indexId, rangeIndex)

      maxRangeK -> metaCR
    }

  }

}