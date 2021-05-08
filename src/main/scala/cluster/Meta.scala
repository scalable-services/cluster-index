package cluster

import java.util.UUID
import scala.collection.concurrent.TrieMap

class Meta[K](override val id: String,
              override val MIN: Int,
              override val MAX: Int)
             (implicit val ord: Ordering[K],
              val parents: TrieMap[String, (Option[Meta[K]], Int)]) extends Block[K]{

  var pointers = Array.empty[Tuple2[K, Block[K]]]

  override def first: K = pointers(0)._1
  override def last: K = pointers(pointers.length - 1)._1

  def setPointers(): Unit = {
    for(i<-0 until pointers.length){
      val (k, c) = pointers(i)
      parents += c.id -> (Some(this), i)
    }
  }

  def left[T <: Block[K]](idx: Int): Option[T] = {
    if(idx == 0) return None
    Some(pointers(idx - 1)._2.asInstanceOf[T])
  }

  def right[T <: Block[K]](idx: Int): Option[T] = {
    if(idx == pointers.length - 1) return None
    Some(pointers(idx + 1)._2.asInstanceOf[T])
  }

  def find(k: K, start: Int, end: Int): (Boolean, Int) = {
    if(start > end) return false -> start

    val pos = start + (end - start)/2
    val c = ord.compare(k, pointers(pos)._1)

    if(c == 0) return true -> pos
    if(c < 0) return find(k, start, pos - 1)

    find(k, pos + 1, end)
  }

  def findPath(k: K): Option[Block[K]] = {
    if(isEmpty()) return None
    val (_, pos) = find(k, 0, pointers.length - 1)
    Some(pointers(if(pos < pointers.length) pos else pos - 1)._2)
  }

  def split(): Meta[K] = {
    val right = new Meta[K](UUID.randomUUID.toString, MIN, MAX)

    val len = pointers.length
    val mid = len/2

    right.pointers = pointers.slice(mid, len)
    pointers = pointers.slice(0, mid)

    setPointers()

    right
  }

  def insert(data: Seq[Tuple2[K, Block[K]]]): (Boolean, Int) = {
    if(isFull()) return false -> 0

    val n = Math.min(MAX - pointers.length, data.length)
    val slice = data.slice(0, n)

    if(slice.exists{case (k, _) => pointers.exists{case (k1, _) => ord.equiv(k, k1)}}){
      return false -> 0
    }

    pointers = (pointers ++ slice).sortBy(_._1)

    setPointers()

    true -> slice.length
  }

  override def length: Int = pointers.length

  override def isFull(): Boolean = pointers.length == MAX
  override def isEmpty(): Boolean = pointers.isEmpty

  override def print(): String = s"${pointers.map(_._1)} -> ${pointers.length}"
}