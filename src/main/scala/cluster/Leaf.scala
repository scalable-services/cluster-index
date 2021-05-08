package cluster

import java.util.UUID
import scala.collection.concurrent.TrieMap

class Leaf[K, V](override val id: String,
                 override val MIN: Int,
                 override val MAX: Int)
                (implicit val ord: Ordering[K],
                 val parents: TrieMap[String, (Option[Meta[K]], Int)]) extends Block[K]{

  var tuples = Array.empty[Tuple2[K, V]]

  override def first: K = tuples(0)._1
  override def last = tuples(tuples.length - 1)._1

  def insert(data: Seq[Tuple2[K, V]]): (Boolean, Int) = {

    if(isFull() || data.isEmpty) return false -> 0

    val n = Math.min(MAX - tuples.length, data.length)
    val slice = data.slice(0, n)

    if(slice.exists{case (k, _) => tuples.exists{case (k1, _) => ord.equiv(k, k1)}}){
      throw new RuntimeException(s"[leaf] ELEMENT ALREADY EXISTS!")
    }

    tuples = (tuples ++ slice).sortBy(_._1)

    true -> slice.length
  }

  def split(): Leaf[K, V] = {
    val right = new Leaf[K, V](UUID.randomUUID.toString, MIN, MAX)

    val len = tuples.length
    val mid = len/2

    right.tuples = tuples.slice(mid, len)
    tuples = tuples.slice(0, mid)

    right
  }

  override def length: Int = tuples.length

  override def isFull(): Boolean = tuples.length == MAX
  override def isEmpty(): Boolean = tuples.isEmpty

  def inOrder(): Seq[Tuple2[K, V]] = tuples

  def remaining: Int = MAX - tuples.length

  override def print(): String = s"${tuples.map(_._1)} -> ${tuples.length}"
}