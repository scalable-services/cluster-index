package cluster

import java.util.UUID
import scala.collection.concurrent.TrieMap

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

  def splitLeaf(left: Leaf[K, V], data: Seq[Tuple2[K, V]]): (Boolean, Int) = {
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

  def insertLeaf(left: Leaf[K, V], data: Seq[Tuple2[K, V]]): (Boolean, Int) = {
    if(left.isFull()){

      /*val right = left.split()
      return handleParent(left, right) -> 0*/

      return splitLeaf(left, data)
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

          // If not the last block (or the only one), filter it
          if(!ord.gt(k, last)){
            list = list.takeWhile{case (k, _) => ord.lt(k, last)}
          }

          insertLeaf(leaf, list)
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