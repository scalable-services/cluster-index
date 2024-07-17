package services.scalable.index.cluster

import org.apache.commons.lang3.RandomStringUtils
import org.scalatest.flatspec.AnyFlatSpec
import services.scalable.index.grpc.IndexContext
import services.scalable.index.impl.MemoryStorage
import services.scalable.index.{Commands, DefaultComparators, DefaultSerializers, IndexBuilder}

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

class RangesSpec extends AnyFlatSpec with Repeatable {

  override val times: Int = 1

  type K = String
  type V = String

  val MAX_ITEMS = 10
  val rand = ThreadLocalRandom.current()

  "it" should "run successfully" in {

    val storage = new MemoryStorage()

    val indexContext = IndexContext()
      .withId(UUID.randomUUID().toString)
      .withLevels(0)
      .withNumElements(0L)
      .withMaxNItems(MAX_ITEMS)
      .withNumLeafItems(MAX_ITEMS)
      .withNumMetaItems(MAX_ITEMS)
      .withLastChangeVersion(UUID.randomUUID().toString)

    val rangeBuilder = IndexBuilder.create[K, V](global, DefaultComparators.ordString,
        indexContext.numLeafItems, indexContext.numMetaItems, indexContext.maxNItems,
        DefaultSerializers.stringSerializer, DefaultSerializers.stringSerializer)
      .storage(storage)
      .serializer(ClusterSerializers.grpcStringStringSerializer)
      .keyToStringConverter(k => k)
      .build()

    var randomData = Seq.empty[K]

    for(i<-0 until 1000){
      val k = RandomStringUtils.randomAlphabetic(4).toLowerCase

      if(!randomData.exists{x => rangeBuilder.ord.equiv(k, x) }){
        randomData = randomData :+ k
      }
    }

    val version = "v1"
    val left = new Range[K, V](indexContext)(rangeBuilder)

    val (randomDataFirstPart, randomDataSecondPart) = randomData.sorted.splitAt(randomData.length/2)

    var dataLeft = Seq.empty[(String, String)]

    for(i<-0 until MAX_ITEMS){
      val k = randomDataFirstPart(i)//RandomStringUtils.randomAlphabetic(10).toLowerCase()
      val v = k.reverse

      if(!dataLeft.exists{case (k1, _) => rangeBuilder.ord.equiv(k1, k)}){
        dataLeft = dataLeft :+ (k, v)
      }
    }

    val result = Await.result(left.execute(Seq(Commands.Insert("index", dataLeft.map{case (k, v) => (k, v, true)},
      Some(version)))), Duration.Inf)

    val leftIndexData = left.inOrderSync()

    assert(leftIndexData.map(_._1) == dataLeft.map(_._1).sorted)

    val indexContext2 = indexContext.withId("index2")
    val right = new Range[K, V](indexContext2)(rangeBuilder)

    var dataRight = Seq.empty[(K, V)]

    for(i<-0 until MAX_ITEMS){
      val k = randomDataSecondPart(i)//RandomStringUtils.randomAlphabetic(10).toLowerCase()
      val v = k.reverse

      if(!dataRight.exists{case (k1, _) => rangeBuilder.ord.equiv(k1, k)}){
        dataRight = dataRight :+ (k, v)
      }
    }

    val result3 = Await.result(right.execute(Seq(Commands.Insert("index", dataRight.map{case (k, v) => (k, v, true)},
      Some(version)))), Duration.Inf)

    val rightIndexData = right.inOrderSync()

    val toRemove = scala.util.Random.shuffle(rightIndexData).slice(0, /*rand.nextInt(1, dataLeft.length/2)*/dataLeft.length/2 + 2)

    val result1 = Await.result(right.execute(Seq(Commands.Remove("index",
      toRemove.map{case (k, v, _) => (k, Some(version))}))),
      Duration.Inf)

    dataRight = dataRight.filterNot{case (k, v) => toRemove.exists{case (k1, _, _) => rangeBuilder.ord.equiv(k, k1) }}

    assert(left.inOrderSync().map(_._1) == dataLeft.map(_._1).sorted)
    assert(right.inOrderSync().map(_._1) == dataRight.map(_._1).sorted)

    /*val merged = Await.result(right.merge(left, version), Duration.Inf)
    val mergedData = dataLeft.map(_._1) ++ dataRight.map(_._1)
    val indexMergedData = merged.inOrderSync().map(_._1)
    assert(merged.inOrderSync().map(_._1) == indexMergedData && merged.ctx.num_elements == indexMergedData.length)*/

    //Await.result(right.borrow(left, version), Duration.Inf)

    println("before: ", left.ctx.num_elements, right.ctx.num_elements)

    val merged = Await.result(left.borrow(right, version), Duration.Inf)

    println("after: ", left.ctx.num_elements, right.ctx.num_elements)

    val both = (dataLeft ++ dataRight).map(_._1)
    val borrowData = left.inOrderSync().map(_._1) ++ right.inOrderSync().map(_._1)
    val mergedData = right.inOrderSync().map(_._1)

    assert(borrowData == both)
    assert(mergedData.length >= MAX_ITEMS/2)

    storage.close()
  }
}
