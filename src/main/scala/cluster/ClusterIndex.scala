package cluster

import services.scalable.index.grpc.{DBContext, IndexView}
import services.scalable.index.{Block, Cache, Commands, Context, DB, IdGenerator, Serializer, Storage, Tuple}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class ClusterIndex[K, V](val metaContext: DBContext,
                         val MAX_ENTRIES: Int,
                         val NUM_LEAF_ENTRIES: Int,
                         val NUM_META_ENTRIES: Int)(implicit val ec: ExecutionContext,
                                                     val storage: Storage,
                                                     val blockSerializer: Serializer[Block[K, V]],
                                                     val dbCtxSerializer: Serializer[Block[K, DBContext]],
                                                     val cache: Cache,
                                                     val ord: Ordering[K],
                                                     val idGenerator: IdGenerator) {

  val meta = new DB[K, DBContext](metaContext)

  meta.createIndex("main", NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
  //meta.createHistory("history", NUM_LEAF_ENTRIES, NUM_META_ENTRIES)

  def findPath(k: K): Future[Option[Range[K, V]]] = {
    val indexOpt = meta.findLatestIndex("main")

    if(indexOpt.isEmpty) return Future.successful(None)

    val index = indexOpt.get

    index.findPath(k).map {
      case None => None
      case Some(leaf) => leaf.find(k) match {
        case None => None
        case Some((_, dbCtx)) => Some(new Range[K, V](dbCtx, MAX_ENTRIES))
      }
    }
  }

  def insertMeta(left: Range[K, V]): Future[Int] = {
    val leftIndex = left.db.findLatestIndex("main").get
    val metaIndex = meta.findLatestIndex("main").get

    leftIndex.max().flatMap { lm =>
      metaIndex.insert(Seq(
        lm.get._1 -> left.db.ctx
      ))
    }
  }

  def insertMeta(left: Range[K, V], right: Range[K, V]): Future[Int] = {
    val leftIndex = left.db.findLatestIndex("main").get
    val rightIndex = right.db.findLatestIndex("main").get
    val metaIndex = meta.findLatestIndex("main").get

    Future.sequence(Seq(leftIndex.max(), rightIndex.max())).flatMap { maxes =>
      val lm = maxes(0).get._1
      val rm = maxes(1).get._1

      metaIndex.insert(Seq(
        lm -> left.db.ctx,
        rm -> right.db.ctx
      ))
    }
  }

  def insertEmpty(data: Seq[Tuple[K,V]], upsert: Boolean): Future[Int] = {

    val range = new Range[K, V](DBContext(UUID.randomUUID().toString), MAX_ENTRIES)
    val db = range.db

    db.createIndex("main", NUM_META_ENTRIES, NUM_META_ENTRIES)
    //db.createHistory("history", NUM_LEAF_ENTRIES, NUM_META_ENTRIES)

    val list = if(data.length <= MAX_ENTRIES) data else data.slice(0, MAX_ENTRIES)

    val cmd = Commands.Insert("main", list)

    range.execute(Seq(cmd)).flatMap { n =>
      insertMeta(range).map(_ => list.length)
    }
  }

  def insertRange(left: Range[K, V], list:  Seq[Tuple[K,V]], upsert: Boolean): Future[Int] = {
    val cmd = Commands.Insert("main", list)

    left.execute(Seq(cmd)).flatMap { n =>
      if(left.isFull()){
        left.split().flatMap { right =>
          insertMeta(left, right).map(_ => 0)
        }
      } else {
        Future.successful(list.length)
      }
    }
  }

  def insert(data: Seq[Tuple[K,V]], upsert: Boolean = false)(implicit ord: Ordering[K]): Future[Int] = {
    val sorted = data.sortBy(_._1)

    val len = sorted.length
    var pos = 0

    def insert(): Future[Int] = {
      if(pos == len) return Future.successful(sorted.length)

      var list = sorted.slice(pos, len)
      val (k, _) = list(0)

      findPath(k).flatMap {
        case None => insertEmpty(list, upsert)
        case Some(range) =>

          val index = range.db.findLatestIndex("main").get

          index.max().flatMap { opt =>
            val last = opt.get._1
            val idx = list.indexWhere{case (k, _) => ord.gt(k, last)}
            if(idx > 0) list = list.slice(0, idx)

            insertRange(range, list, upsert)
          }
      }.flatMap { n =>
        println(s"\ninserted: ${n}\n")
        pos += n
        insert()
      }
    }

    insert()
  }
}
