package cluster

import cluster.grpc.KeyIndexContext
import com.google.protobuf.ByteString
import services.scalable.index.grpc.{IndexContext, RootRef}
import services.scalable.index.{AsyncIterator, Block, Bytes, Cache, Errors, IdGenerator, InsertionResult, QueryableIndex, Serializer, Storage, Tuple}

import java.util.UUID
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import services.scalable.index.DefaultPrinters._
import Printers._

class ClusterIndex[K, V](val metaContext: IndexContext,
                         val maxNItems: Int,
                         val numLeafItems: Int,
                         val numMetaItems: Int
                        )(implicit val ec: ExecutionContext,
                          val storage: Storage,
                          val blockSerializer: Serializer[Block[K, V]],
                          val dbCtxSerializer: Serializer[Block[K, KeyIndexContext]],
                          val cache: Cache,
                          val ord: Ordering[K],
                          val ks: K => String,
                          val vs: V => String,
                          val kics: KeyIndexContext => String,
                          val idGenerator: IdGenerator) {
  val meta = new QueryableIndex[K, KeyIndexContext](metaContext)

  var indexes = TrieMap.empty[String, QueryableIndex[K, V]]

  def save(clear: Boolean = true): Future[IndexContext] = {
    Future.sequence(indexes.map { case (id, index) =>
      println(s"saving index ${index.ctx.indexId}")

      TestHelper.loadOrCreateIndex(index.ctx.snapshot(false)).flatMap { _ =>
        index.save(clear)
      }

    }).flatMap { ok =>
      meta.save(clear)
    }
  }

  def saveIndexes(clear: Boolean = true): Future[Boolean] = {
    Future.sequence(indexes.map { case (id, index) =>
      println(s"saving index[2] ${index.ctx.indexId}")

      TestHelper.loadOrCreateIndex(index.ctx.snapshot(false)).flatMap { _ =>
        index.save(clear)
      }

    }).map(_.toSeq.length == indexes.size)
  }

  def findPath(k: K): Future[Option[(K, QueryableIndex[K, V])]] = {
    meta.findPath(k).map {
      case None => None
      case Some(leaf) =>
        val (_, (key, kctx, _)) = leaf.findPath(k)

       // val idxs = this.indexes
       // val idx = indexes(ictx.id)

        if(!indexes.isDefinedAt(kctx.ctxId)){
          println()
        }

        //val ictx = Await.result(storage.loadIndex(kctx.ctxId), Duration.Inf).get
        val index = indexes.get(kctx.ctxId).get//.getOrElse(new QueryableIndex[K, V](ictx))

        Some(key -> index)
    }
  }

  def insertMeta(left: QueryableIndex[K, V]): Future[InsertionResult] = {
    println(s"insert indexes in meta[1]: left ${left.ctx.indexId}")

    left.max().flatMap { lm =>
      meta.insert(Seq(Tuple3(lm.get._1, KeyIndexContext(ByteString.copyFrom(lm.get._1.asInstanceOf[Bytes]),
        left.ctx.indexId), true)))
    }
  }

  def insertMeta(left: QueryableIndex[K, V], right: QueryableIndex[K, V], last: K): Future[InsertionResult] = {

    Future.sequence(Seq(left.max(), right.max())).flatMap { maxes =>
      val lm = maxes(0).get._1
      val rm = maxes(1).get._1

      println(s"inserting indexes in meta[2]: left ${left.ctx.indexId} right: ${right.ctx.indexId}")

      meta.remove(Seq(Tuple2(last, None))).flatMap { ok =>
        meta.insert(Seq(
          Tuple3(lm, KeyIndexContext(ByteString.copyFrom(lm.asInstanceOf[Bytes]),
            left.ctx.indexId), true),
          Tuple3(rm, KeyIndexContext(ByteString.copyFrom(rm.asInstanceOf[Bytes]),
            right.ctx.indexId), true)
        ))
      }
    }
  }

  def insertEmpty(data: Seq[Tuple3[K, V, Boolean]]): Future[Int] = {
    val leftN = Math.min(maxNItems, data.length)
    val slice = data.slice(0, leftN)

    val index = new QueryableIndex[K, V](
      IndexContext()
      .withId(UUID.randomUUID().toString)
      .withMaxNItems(maxNItems)
      .withNumElements(0)
      .withLevels(0)
      .withNumLeafItems(numLeafItems)
      .withNumMetaItems(numMetaItems))

    indexes.put(index.ctx.indexId, index)

    println(s"inserted index ${index.ctx.indexId}")

    index.insert(slice).flatMap { _ =>
      //println()
      insertMeta(index).map(_ => slice.length)
    }
  }

  def split(left: QueryableIndex[K, V]): (QueryableIndex[K, V], QueryableIndex[K, V]) = {

    var leftR = Await.result(left.ctx.getMeta(left.ctx.root.get), Duration.Inf)

    if (leftR.length == 1) {
      leftR = Await.result(left.ctx.getMeta(leftR.pointers(0)._2.unique_id), Duration.Inf)
    }

    val leftN = leftR.pointers.slice(0, leftR.length / 2).map { case (_, ptr) =>
      ptr.nElements
    }.sum

    val rightN = leftR.pointers.slice(leftR.length / 2, leftR.length).map { case (_, ptr) =>
      ptr.nElements
    }.sum

    val leftICtx = left.c
      .withId(left.ctx.id)
      .withMaxNItems(left.c.maxNItems)
      .withNumElements(leftN)
      .withLevels(leftR.level)
      .withNumLeafItems(left.c.numLeafItems)
      .withNumMetaItems(left.c.numMetaItems)

    val rightICtx = left.c
      .withId(UUID.randomUUID().toString)
      .withMaxNItems(left.c.maxNItems)
      .withNumElements(rightN)
      .withLevels(leftR.level)
      .withNumLeafItems(left.c.numLeafItems)
      .withNumMetaItems(left.c.numMetaItems)

    val lindex = new QueryableIndex[K, V](leftICtx)
    val rindex = new QueryableIndex[K, V](rightICtx)

    val leftRoot = leftR.copy()(lindex.ctx)
    lindex.ctx.root = Some(leftRoot.unique_id)

    val rightRoot = leftRoot.split()(rindex.ctx)
    rindex.ctx.root = Some(rightRoot.unique_id)

    /*val ileft = Await.result(TestHelper.all(left.inOrder()), Duration.Inf).map { case (k, v, _) => k -> v }
    logger.debug(s"${Console.BLUE_B}idata data: ${ileft.map { case (k, v) => new String(k, Charsets.UTF_8) -> new String(v) }}${Console.RESET}\n")

    val idataL = Await.result(TestHelper.all(lindex.inOrder()), Duration.Inf).map { case (k, v, _) => k -> v }
    logger.debug(s"${Console.GREEN_B}idataL data: ${idataL.map { case (k, v) => new String(k, Charsets.UTF_8) -> new String(v) }}${Console.RESET}\n")

    val idataR = Await.result(TestHelper.all(rindex.inOrder()), Duration.Inf).map { case (k, v, _) => k -> v }
    logger.debug(s"${Console.GREEN_B}idataR data: ${idataR.map { case (k, v) => new String(k, Charsets.UTF_8) -> new String(v) }}${Console.RESET}\n")

    assert(idataR.slice(0, idataL.length) != idataL)
    assert(ileft == (idataL ++ idataR))*/

    (lindex, rindex)
  }

  def copy(index: QueryableIndex[K, V]): QueryableIndex[K, V] = {
    val context = IndexContext(index.ctx.indexId, index.c.numLeafItems, index.c.numMetaItems,
      index.ctx.root.map { r => RootRef(r._1, r._2) }, index.ctx.levels, index.ctx.num_elements,
      index.c.maxNItems)

    val copy = new QueryableIndex[K, V](context)(ec,
      index.storage, index.serializer, index.cache, index.ord, index.idGenerator, ks, vs)

    index.ctx.blockReferences.foreach { case (id, _) =>
      copy.ctx.blockReferences += id -> id
    }

    copy
  }

  def insertRange(left: QueryableIndex[K, V], list: Seq[Tuple3[K, V, Boolean]], last: K): Future[Int] = {

    val lindex = left.copy()//copy(left)

    //val refs = left.ctx.blockReferences

    val remaining = lindex.ctx.maxNItems - lindex.ctx.num_elements
    val n = Math.min(remaining, list.length).toInt
    val slice = list.slice(0, n)

    if(remaining == 0){
      //val original = Await.result(all(left.inOrder()), Duration.Inf).map{x => x._1 -> x._2}

      //println("ctx", left.ctx.num_elements, left.ctx.levels)

      return for {
        rindex <- lindex.split().map { rindex =>

          indexes.put(lindex.ctx.indexId, lindex)

          println(s"inserted index ${rindex.ctx.id} with left being: ${lindex.ctx.indexId}")

          indexes.put(rindex.ctx.indexId, rindex)
          rindex
        }

        // lindex.ctx.blockReferences ++= refs

        n <- insertMeta (lindex, rindex, last).map { n =>

          //println("meta number of elems: ", meta.ctx.num_elements)

          //val listL = Await.result(all(lindex.inOrder()), Duration.Inf).map { x => x._1 -> x._2 }
          //val listR = Await.result(all(rindex.inOrder()), Duration.Inf).map { x => x._1 -> x._2 }

          //println(s"after splitting $n left", listL.map{x => new String(x._1.asInstanceOf[Bytes])}, "right: ",
          //listR.map{x => new String(x._1.asInstanceOf[Bytes])})

          //assert((listL ++ listR) == original)

          //println("yet to insert", list.map { x => new String(x._1.asInstanceOf[Bytes]) })

          //println()
          0
        }

      } yield {
        n
      }
    }

    println(s"insert normally ", slice.map{x => new String(x._1.asInstanceOf[Bytes])})

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

        meta.remove(Seq(Tuple2(last, None))).flatMap { ok =>
          meta.insert(Seq(
            Tuple3(lm, KeyIndexContext(ByteString.copyFrom(lm.asInstanceOf[Bytes]),
              lindex.ctx.indexId), true)
          ))
        }
      }.map(_ => slice.length)
    }
  }

  def insert(data: Seq[Tuple3[K, V, Boolean]])(implicit ord: Ordering[K]): Future[Int] = {
    val sorted = data.sortBy(_._1)

    if (sorted.exists { case (k, _, _) => sorted.count { case (k1, _, _) => ord.equiv(k, k1) } > 1 }) {
      return Future.failed(Errors.DUPLICATED_KEYS(data.map(_._1), ks))
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

            //val list2 = inOrder()

            //println(s"inserted $n", list2.map{x => new String(x._1.asInstanceOf[Bytes])})
            //assert(list2.map{x => x._1 -> x._2} == list.slice(0, n))

            n
          }
        case Some((last, index)) =>

          //val x = Await.result(all(index.inOrder()), Duration.Inf).map{ x => new String(x._1.asInstanceOf[Bytes])}
          //println(s"insert existing", new String(last.asInstanceOf[Bytes]), "i ", x)

          val idx = list.indexWhere { case (k, _, _) => ord.gt(k, last) }
          if (idx > 0) list = list.slice(0, idx)

          insertRange(index, list, last)
      }.flatMap { n =>
        println(s"\ninserted: ${n}\n")
        pos += n
        insert()
      }
    }

    insert()
  }

  def update(data: Seq[Tuple3[K, V, Option[String]]])(implicit ord: Ordering[K]): Future[Int] = {

    val sorted = data.sortBy(_._1)

    if (sorted.exists { case (k, _, _) => sorted.count { case (k1, _, _) => ord.equiv(k, k1) } > 1 }) {
      return Future.failed(Errors.DUPLICATED_KEYS(sorted.map(_._1), ks))
    }

    val len = sorted.length
    var pos = 0

    def update(): Future[Int] = {
      if (len == pos) return Future.successful(sorted.length)

      var list = sorted.slice(pos, len)
      val (k, _, _) = list(0)

      findPath(k).flatMap {
        case None => Future.failed(Errors.KEY_NOT_FOUND(k, ks))
        case Some((last, index)) =>

          val idx = list.indexWhere { case (k, _, _) => ord.gt(k, last) }
          if (idx > 0) list = list.slice(0, idx)

          index.update(list)
      }.flatMap { res =>
        pos += res.n
        update()
      }
    }

    update()
  }

  /*def execute(cmds: Seq[Commands.Command[K, V]]): Future[Boolean] = {

  }*/

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

  def inOrder(): Seq[(K, V, String)] = {
    val iter = Await.result(all(meta.inOrder()), Duration.Inf)

    println(s"${Console.CYAN_B}meta keys: ${iter.map(x => new String(x._1.asInstanceOf[Array[Byte]]))}${Console.RESET}")

    iter.map { case (k, link, version) =>
      val ctx = Await.result(storage.loadIndex(link.ctxId), Duration.Inf).get

      println(s"range n: ${ctx.numElements}")
      val r = Await.result(all(new QueryableIndex[K, V](ctx).inOrder()), Duration.Inf)

      r
    }.flatten
  }

  def inOrderFromSaved(): Seq[(K, V, String)] = {
    val iter = Await.result(all(meta.inOrder()), Duration.Inf)

    println(s"${Console.CYAN_B}meta keys: ${iter.map(x => new String(x._1.asInstanceOf[Array[Byte]]))}${Console.RESET}")

    iter.map { case (k, link, version) =>

      val ctx = Await.result(storage.loadIndex(link.ctxId), Duration.Inf).get

      val link2 = Await.result(storage.loadIndex(ctx.id), Duration.Inf).get
      val r = Await.result(all(new QueryableIndex[K, V](link2).inOrder()), Duration.Inf)

      println(s"range ${ctx.id} n: ${ctx.numElements}")

      r
    }.flatten
  }
}

object ClusterIndex {

  def fromRange(rangeId: String,
                      numLeafItems: Int,
                      numMetaItems: Int)(implicit ec: ExecutionContext,
                       storage: Storage,
                       blockSerializer: Serializer[Block[Bytes, Bytes]],
                       dbCtxSerializer: Serializer[Block[Bytes, KeyIndexContext]],
                       cache: Cache,
                       ord: Ordering[Bytes],
                       idGenerator: IdGenerator): Future[(Bytes, ClusterIndex[Bytes, Bytes])] = {

    val metaCtx = IndexContext()
      .withId(UUID.randomUUID.toString)
      .withMaxNItems(Int.MaxValue)
      .withLevels(0)
      .withNumLeafItems(numLeafItems)
      .withNumMetaItems(numMetaItems)

    val metaRange = new QueryableIndex[Bytes, KeyIndexContext](metaCtx)

    for {
      rangeCtx <- storage.loadIndex(rangeId).map(_.get)

      rangeIndex = new QueryableIndex[Bytes, Bytes](rangeCtx)

      maxRangeK <- rangeIndex.max().map(_.get).map(_._1)

      n <- metaRange.insert(Seq(
        Tuple3(maxRangeK, KeyIndexContext(ByteString.copyFrom(maxRangeK), rangeCtx.id), true)
      ))

    } yield {

      val clusterRangeCtx = metaRange.ctx.snapshot(false)

      val metaCR = new ClusterIndex[Bytes, Bytes](clusterRangeCtx, 256,
        clusterRangeCtx.numLeafItems, clusterRangeCtx.numMetaItems)

      metaCR.indexes.put(rangeIndex.ctx.indexId, rangeIndex)

      maxRangeK -> metaCR
    }

  }

}