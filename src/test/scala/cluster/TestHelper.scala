package cluster

import services.scalable.index.{AsyncIterator, Block, Context, Leaf, Meta, Tuple}
import scala.concurrent.{ExecutionContext, Future}

object TestHelper {

  def inOrder[K, V](ctxId: String, start: Block[K,V])(implicit ctx: Context[K, V], ec: ExecutionContext): Future[Seq[Tuple[K,V]]] = {
    start match {
      case leaf: Leaf[K,V] => Future.successful(leaf.inOrder())
      case meta: Meta[K,V] =>

        val pointers = meta.pointers
        val len = pointers.length

        var data = Seq.empty[(Int, Future[Seq[Tuple[K,V]]])]

        for(i<-0 until len){
          val (_, c) = pointers(i)

          //ctx.setParent(c, i, Some(meta))

          data = data :+ i -> ctx.get(c.unique_id).flatMap{b => inOrder(ctxId, b)}
        }

        //meta.setPointers()

        Future.sequence(data.sortBy(_._1).map(_._2)).map { results =>
          results.flatten
        }
    }
  }

  def inOrder[K, V](ctxId: String, root: (String, String))(implicit ctx: Context[K, V], ec: ExecutionContext): Future[Seq[Tuple[K,V]]] = {
    ctx.get(root).flatMap{b => inOrder(ctxId, b)}
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
