package cluster

import com.google.common.primitives.UnsignedBytes
import org.apache.commons.lang3.RandomStringUtils

import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicReference
import scala.language.postfixOps

class MainSpec extends Repeatable {

  override val times = 1

  "index data " must "be equal to list data" in {

    val rand = ThreadLocalRandom.current()

    implicit val ord = new Ordering[Bytes] {
      val c = UnsignedBytes.lexicographicalComparator()
      override def compare(x: Bytes, y: Bytes): Int = c.compare(x, y)
    }

    val ref = new AtomicReference[Option[String]](None)
    implicit val cache = new MemoryCache()
    var data = Seq.empty[Tuple]

    val NUM_LEAF_ELEMENTS = 8
    val NUM_META_ELEMENTS = 8

    val index = new Index(None, NUM_LEAF_ELEMENTS, NUM_META_ELEMENTS)

    def insert(): Unit = {
      val n = rand.nextInt(1, 1000)

      var list = Seq.empty[(Bytes, Bytes)]

      for(i<-0 until n){
        val e = RandomStringUtils.randomAlphanumeric(rand.nextInt(5, 10)).getBytes()
        list = list :+ e -> e
      }

      if(index.insert(list)._1 && cache.save(index.ctx)){
        data = data ++ list
      }
    }

    def remove(): Unit = {
      if(data.isEmpty) return

      val list = if(data.length > 2) scala.util.Random.shuffle(data.slice(0, rand.nextInt(1, data.length)))
      else data

      if(index.remove(list.map(_._1))._1 && cache.save(index.ctx)){
        data = data.filterNot{case (k, _) => list.exists{case (k1, _) => ord.equiv(k, k1)}}
      }
    }

    def update(): Unit = {
      var list = index.inOrder()

      if(list.isEmpty) return

      list = if(list.length > 2) scala.util.Random.shuffle(list.slice(0, rand.nextInt(1, list.length)))
      else list

      list = list.map{case (k, _) => k -> RandomStringUtils.randomAlphanumeric(5, 10).getBytes()}

      if(index.update(list)._1 && cache.save(index.ctx)){
        data = data.filterNot{case (k, _) => list.exists{case (k1, _) => ord.equiv(k, k1)}}
        data = data ++ list
      }
    }

    for(i<-0 until 10){
      rand.nextInt(1, 3) match {
        case 1 => insert()
        case 2 => remove()
        case _ => update()
      }
    }

    val dsorted = data.sortBy(_._1)
    val isorted = index.inOrder()

    //index.prettyPrint()

    println(s"dsorted: ${dsorted.map{case (k, v) => new String(k) -> new String(v)}}\n")
    println(s"isorted: ${isorted.map{case (k, v) => new String(k) -> new String(v)}}\n")

    assert(isorted.equals(dsorted))
  }

}
