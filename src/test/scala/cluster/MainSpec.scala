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
