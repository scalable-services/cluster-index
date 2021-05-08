package cluster

import com.google.common.primitives.UnsignedBytes
import org.apache.commons.lang3.RandomStringUtils
import org.scalatest.flatspec.AnyFlatSpec

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import scala.collection.concurrent.TrieMap

class MainSpec extends AnyFlatSpec with Repeatable {

  override val times = 1

  val rand = ThreadLocalRandom.current()
  //val comp = UnsignedBytes.lexicographicalComparator()

  implicit val ord = new Ordering[String] {
    override def compare(x: String, y: String): Int = x.compareTo(y)
  }

  val NUM_LEAF_ENTRIES = rand.nextInt(10, 64)
  /*val NUM_META_ENTRIES = 10

  val MIN = NUM_LEAF_ENTRIES/2
  val MAX = NUM_LEAF_ENTRIES*/

  /*implicit val ord = new Ordering[Array[Byte]] {
    override def compare(x: Array[Byte], y: Array[Byte]): Int = comp.compare(x, y)
  }*/

  "it " should " successfully perform operations" in {

    trait Block[K] {
      val id: String

      val MIN: Int
      val MAX: Int

      def first: K
      def last: K

      def length: Int

      def isFull(): Boolean
      def isEmpty(): Boolean

      def print(): String
    }

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

    class Index[K, V](val NUM_ELEMENTS: Int)(implicit val ord: Ordering[K]) {

      val MAX = NUM_ELEMENTS
      val MIN = MAX/2

      var root: Option[Block[K]] = None
      implicit val parents = TrieMap.empty[String, (Option[Meta[K]], Int)]

      def findPath(k: K, start: Block[K]): Option[Leaf[K, V]] = {
        start match {
          case leaf: Leaf[K, V] => Some(leaf)
          case meta: Meta[K] =>

            meta.setPointers()

            findPath(k, meta.findPath(k).get)
        }
      }

      def findPath(k: K): Option[Leaf[K, V]] = {
        if(root.isEmpty) {
          return None
        }

        val start = root.get

        parents += start.id -> (None, 0)

        findPath(k, start)
      }

      def recursiveCopy(block: Block[K]): Boolean = {
        val (p, pos) = parents(block.id)

        p match {
          case None =>

            root = Some(block)
            parents += block.id -> (None, 0)

            true

          case Some(parent) =>

            parent.pointers(pos) = block.last -> block
            parents += block.id -> (Some(parent), pos)

            recursiveCopy(parent)
        }
      }

      def insertEmpty(data: Seq[Tuple2[K, V]]): (Boolean, Int) = {
        val leaf = new Leaf[K, V](UUID.randomUUID.toString, MIN, MAX)

        parents += leaf.id -> (None, 0)

        val (ok, n) = leaf.insert(data)

        println(n)

        (ok && recursiveCopy(leaf)) -> n
      }

      def insertParent(left: Meta[K], prev: Block[K]): Boolean = {
        if(left.isFull()){
          val right = left.split()

          if(ord.gt(prev.last, left.last)){
            right.insert(Seq(prev.last -> prev))
          } else {
            left.insert(Seq(prev.last -> prev))
          }

          return handleParent(left, right)
        }

        val (ok, n) = left.insert(Seq(prev.last -> prev))

        ok && recursiveCopy(left)
      }

      def handleParent(left: Block[K], right: Block[K]): Boolean = {
        val (p, pos) = parents(left.id)

        p match {
          case None =>

            val meta = new Meta[K](UUID.randomUUID.toString, MIN, MAX)

            parents += meta.id -> (None, 0)

            meta.insert(Seq(
              left.last -> left ,
              right.last -> right
            ))

            recursiveCopy(meta)

            true

          case Some(parent) =>

            parent.pointers(pos) = left.last -> left
            parents += left.id -> (Some(parent), pos)

            insertParent(parent, right)

        }
      }

      def splitLeaf(left: Leaf[K, V], data: Seq[Tuple2[K, V]], check: Boolean): (Boolean, Int) = {
        val right = left.split()

        var count = 0

        assert(!data.isEmpty)

        val (k, _) = data(0)
        val leftLast = left.last
        val rightLast = right.last

        var list = data

        // Avoids searching for the path again! :)
        if(ord.gt(k, leftLast)){

          if(!ord.gt(k, rightLast)){
            list = list.takeWhile{case (k, _) => ord.lt(k, rightLast)}
          }

          val (rok, rn) = right.insert(list)

          count += rn

        } else {
          val (lok, ln) = left.insert(list.takeWhile{case (k, _) => ord.lt(k, leftLast)})
          count += ln
        }

        println(s"count: ${count}")

        handleParent(left, right) -> count
      }

      def insertLeaf(left: Leaf[K, V], data: Seq[Tuple2[K, V]], check: Boolean): (Boolean, Int) = {
        if(left.isFull()){

          /*val right = left.split()
          return handleParent(left, right) -> 0*/

          return splitLeaf(left, data, check)
        }

        val (ok, n) = left.insert(data)
        (ok && recursiveCopy(left)) -> n
      }

      def insert(data: Seq[Tuple2[K, V]]): (Boolean, Int) = {

        val sorted = data.sortBy(_._1)

        val len = sorted.length
        var pos = 0

        while(pos < len){
          var list = sorted.slice(pos, len)
          val (k, _) = list(0)

          val (ok, n) = findPath(k) match {
            case None => insertEmpty(list)
            case Some(leaf) =>

              val last = leaf.last
              var check = false

              // If not the last block (or the only one), filter it
              if(!ord.gt(k, last)){
                check = true
                list = list.takeWhile{case (k, _) => ord.lt(k, last)}
              }

              insertLeaf(leaf, list, check)
          }

          pos += n
        }

        true -> sorted.length
      }

      def inOrder(start: Block[K]): Seq[Tuple2[K, V]] = {
        start match {
          case leaf: Leaf[K, V] => leaf.inOrder()
          case meta: Meta[K] => meta.pointers.foldLeft(Seq.empty[Tuple2[K, V]]){ case (prev, (_, nxt)) =>
            prev ++ inOrder(nxt)
          }
        }
      }

      def inOrder(): Seq[Tuple2[K, V]] = {
        root match {
          case None => Seq.empty[Tuple2[K, V]]
          case Some(start) => inOrder(start)
        }
      }

      def prettyPrint(): Tuple3[Int, Int, Int] = {

        val levels = scala.collection.mutable.Map[Int, scala.collection.mutable.ArrayBuffer[Block[K]]]()
        var num_data_blocks = 0

        def inOrder(start: Block[K], level: Int): Unit = {

          val opt = levels.get(level)
          var l: scala.collection.mutable.ArrayBuffer[Block[K]] = null

          if(opt.isEmpty){
            l = scala.collection.mutable.ArrayBuffer[Block[K]]()
            levels  += level -> l
          } else {
            l = opt.get
          }

          start match {
            case data: Leaf[K, V] =>
              num_data_blocks += 1
              l += data

            case meta: Meta[K] =>

              l += meta

              val length = meta.pointers.length
              val pointers = meta.pointers

              for(i<-0 until length){
                inOrder(pointers(i)._2, level + 1)
              }

          }
        }

        root match {
          case Some(root) => inOrder(root, 0)
          case _ =>
        }

        var degrees = 0

        println("BEGIN BTREE:\n")
        val list = levels.keys.toSeq.sorted

        for(i<-0 until list.length){
          val l = list(i)
          val level = levels(l)

          if(l > 0){
            degrees += level.map(_.length).sum/level.length
          }

          println(s"level[$l]: ${level.map(_.print())}\n")
        }

        /*list.foreach { case level =>
          println(s"level[$level]: ${levels(level).map(_.print())}\n")
        }*/

        println("END BTREE\n")

        Tuple3(levels.size, num_data_blocks, if(levels.size == 0) 0 else if(levels.size - 1 > 0) degrees/(levels.size - 1)
          else levels(0).map(_.length).sum/levels(0).length)
      }
    }

    val index = new Index[String, String](NUM_LEAF_ENTRIES)
    var data = Seq.empty[Tuple2[String, String]]

    for(i<-0 until 10){

      var list = Seq.empty[Tuple2[String, String]]

      for(j<-0 until rand.nextInt(10, 1000)){

        val k = RandomStringUtils.randomAlphabetic(5, 10)

        if(!data.exists{case (k1, v) =>ord.compare(k, k1) == 0 }){
          list = list :+ k -> k
        }

      }

      val (ok, n) = index.insert(list)

      data = data ++ list

      assert(ok && n == list.length)
    }

    val dsorted = data.map(_._1).sorted
    val isorted = index.inOrder().map(_._1)

    println(s"isorted: $isorted\n")
    println(s"dsorted: $dsorted")

    println(s"average dgree: ${index.prettyPrint()}")

    println(s"n: ${dsorted.length} degree: ${index.MIN} - ${index.MAX}")

    assert(isorted.equals(dsorted))
  }

}
