package services.scalable.index.cluster

import org.apache.commons.lang3.RandomStringUtils
import org.scalatest.flatspec.AnyFlatSpec
import services.scalable.index.grpc.IndexContext
import services.scalable.index.impl.{CassandraStorage, MemoryStorage}
import services.scalable.index.{DefaultComparators, DefaultSerializers, IndexBuilder, Leaf}

import java.util
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

class RandomOpsWithErrorsClusterSpec extends AnyFlatSpec with Repeatable {

  override val times: Int = 1

  type K = String
  type V = String

  "it" should "run successfully" in {

    val rand = ThreadLocalRandom.current()
    val MAX_ITEMS = rand.nextInt(128, 512)
    val NUM_LEAF_ENTRIES = rand.nextInt(4, 64)
    val NUM_META_ENTRIES = rand.nextInt(4, 64)

   // val session = TestHelper.getSession()
    val storage = /*new CassandraStorage(session, true)*/ new MemoryStorage()

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

    val version = "v1"
    val clusterIndexId = "ci2"

    Await.result(
      storage.loadOrCreate(IndexContext()
        .withId(clusterIndexId)
        .withLevels(0)
        .withNumElements(0L)
        .withMaxNItems(Long.MaxValue)
        .withNumLeafItems(NUM_LEAF_ENTRIES)
        .withNumMetaItems(NUM_META_ENTRIES)
        .withLastChangeVersion(UUID.randomUUID().toString)),
      Duration.Inf
    )

    var data = Seq.empty[(K, V)]

    def insert(): Unit = {
      val n = rand.nextInt(100, 1000)

      var list = (0 until n).map { _ =>
        val k = RandomStringUtils.randomAlphabetic(10).toLowerCase()
        val v = k

        (k, v, true)
      }.filterNot{case (k, _, _) => data.exists{case (k1, _) => rangeBuilder.ord.equiv(k, k1)}}

      if(list.isEmpty) {
        println("no unique data to insert!")
        return
      }

      val ctx = Await.result(storage.loadIndex(clusterIndexId), Duration.Inf).get
      val cindex = new ClusterIndex[K, V](ctx)(rangeBuilder)

      val insertDups = rand.nextInt(1, 6) == 3

      println(s"${Console.GREEN}INSERTING...${Console.RESET}")

      if(insertDups){
        list = list :+ list.head
      }

      val r1 = Await.result(cindex.insert(list, version), Duration.Inf)

      //assert(r1.success)

      if(!r1.success){
        println(s"${Console.RED_B}INSERTION ERROR${Console.RESET}")
        return
      }

      val r2 = Await.result(cindex.save(), Duration.Inf)

      data = data ++ list.map{case (k, v, _) => k -> v}

      println(s"${Console.GREEN_B}INSERTED ${list.length}${Console.RESET}")

      ///if(list.isEmpty) return Seq.empty[Commands.Insert[K, V]]

      //Seq(Commands.Insert(clusterIndexId, list, Some(version)))
    }

    def remove(): Unit = {

      if(data.isEmpty) {
        println("no data to remove! Index is empty already!")
        return
      }

      val dataOrderedBefore = data.map(_._1).sorted.toList

      val keys = data.map(_._1)
      var toRemoveRandom = (if(keys.length > 1) scala.util.Random.shuffle(keys).slice(0, rand.nextInt(1, keys.length))
        else keys).map { _ -> Some(version)}

      val removeNonExisting =  rand.nextInt(1, 6)

      toRemoveRandom = removeNonExisting match {
        case 1 =>
          val randomK = RandomStringUtils.randomAlphabetic(10)

          println(s"random key to remove: ${randomK}")

          toRemoveRandom :+ (randomK, Some(version))

          //Seq((randomK, Some(version)))

        case 3 if toRemoveRandom.length > 1 =>

          val (k, v) = toRemoveRandom(0)
          toRemoveRandom.slice(1, toRemoveRandom.length) :+ (k, Some(UUID.randomUUID().toString))

        case _ => toRemoveRandom
      }

      val ctx = Await.result(storage.loadIndex(clusterIndexId), Duration.Inf).get
      val cindex = new ClusterIndex[K, V](ctx)(rangeBuilder)

      val metaBefore = Await.result(cindex.meta.all(cindex.meta.inOrder()), Duration.Inf)
      val metaBeforeV = metaBefore.map { x =>
        Await.result(cindex.getRange(x._2.rangeId), Duration.Inf).inOrderSync().map(_._1)
      }.flatten

      println(s"meta before: ${metaBefore.length}")

      println(s"${Console.MAGENTA_B} THREAD ID: ${Thread.currentThread().threadId()} ${Console.RESET}")

      val r1 = Await.result(cindex.remove(toRemoveRandom, version), Duration.Inf)

      //val r1 = ClusterResult.RemovalResult(false, 0, Some(new RuntimeException(("not ook"))))

      if(!r1.success){

        println("not removed...")

        val ctx2 = Await.result(storage.loadIndex(clusterIndexId), Duration.Inf).get

        val cindex2 = new ClusterIndex[K, V](ctx2)(rangeBuilder)

        val metaAfter = Await.result(cindex2.meta.all(cindex2.meta.inOrder()), Duration.Inf)

        val isMetaKeysEqual = metaBefore.map(_._1).toList == metaAfter.map(_._1).toList
        val isMetaValuesEqual = metaBefore.map(x => x._2.lastChangeVersion)
          .toList == metaAfter.map(_._2.lastChangeVersion).toList


        val metaAfterV = metaAfter.map { x =>
          Await.result(cindex2.getRange(x._2.rangeId), Duration.Inf).inOrderSync().map(_._1)
        }.flatten

        val ciEquals = metaBeforeV == metaAfterV

        val inOrder = cindex2.inOrder().map(_._1).toList
        val dataOrdered = data.map(_._1).sorted.toList

        if(inOrder != dataOrdered){
          metaBefore
          metaAfter
          isMetaKeysEqual
          isMetaValuesEqual
          ciEquals

          assert(false)
        }

        println(s"${Console.RED_B}REMOVAL ERROR${Console.RESET}")
        return
      }

      val r2 = Await.result(cindex.save(), Duration.Inf)

      data = data.filterNot{case (k, v) => toRemoveRandom.exists{case (k1, _) => rangeBuilder.ord.equiv(k, k1)}}

      println(s"${Console.MAGENTA_B}REMOVED ${toRemoveRandom.length}${Console.RESET}")
    }

    def update(): Unit = {

      if(data.isEmpty) {
        println("no data to update! Index is empty!")
        return
      }

      var toUpdateRandom = (if(data.length > 1) scala.util.Random.shuffle(data).slice(0, rand.nextInt(1, data.length))
        else data).map { case (k, v) => (k, RandomStringUtils.randomAlphabetic(10), Some(version))}

      val ctx = Await.result(storage.loadIndex(clusterIndexId), Duration.Inf).get
      val cindex = new ClusterIndex[K, V](ctx)(rangeBuilder)

      val updateNonExisting = rand.nextInt(1, 6)

      toUpdateRandom = updateNonExisting match {
        case 1 => toUpdateRandom :+ (RandomStringUtils.randomAlphabetic(10),
          RandomStringUtils.randomAlphabetic(10), Some(version))
        case 3 if toUpdateRandom.length > 1 =>
          val (k, value, vs) = toUpdateRandom(0)
          toUpdateRandom.slice(1, toUpdateRandom.length) :+ (k, value, Some(UUID.randomUUID().toString))

        case _ => toUpdateRandom
      }

      println(s"${Console.CYAN}UPDATING...${Console.RESET}")

      val r1 = Await.result(cindex.update(toUpdateRandom, version), Duration.Inf)

      if(!r1.success){
        println(s"${Console.RED_B}UPDATE ERROR${Console.RESET}")
        return
      }

      val r2 = Await.result(cindex.save(), Duration.Inf)

      data = data.filterNot{case (k, v) => toUpdateRandom.exists{case (k1, _, _) => rangeBuilder.ord.equiv(k, k1)}}
      data = data ++ toUpdateRandom.map{case (k, v, _) => k -> v}

      println(s"${Console.CYAN_B}UPDATED ${toUpdateRandom.length}${Console.RESET}")
    }

    val n = 100

    /*for(i<-0 until n){
      rand.nextInt(1, 100)  match {
        case i if i % 3 == 0 => remove()
       // case i if i % 5 == 0 => update()
        case _ => insert()
      }
    }*/

    val list = (0 until 100).map{_ => rand.nextInt(1, 10000)}.toList

    list.foreach {
      case i if i % 3 == 0 => remove()
      case i if i % 5 == 0 => update()
      case _ => insert()
    }

    val ctx = Await.result(storage.loadIndex(clusterIndexId), Duration.Inf).get
    val cindex = new ClusterIndex[K, V](ctx)(rangeBuilder)

    val dataOrdered = data.iterator.toSeq.sortBy(_._1)/*.map(_._1)*/.toList
    val indexOrdered = cindex.inOrder()/*.map(_._1)*/.toList

    val notSameData = dataOrdered.filterNot(x => rangeBuilder.ord.equiv(x._1, x._2))
    val notSameIndex =  indexOrdered.filterNot(x => rangeBuilder.ord.equiv(x._1, x._2))

    println(s"dataordered: ${dataOrdered.map(k => rangeBuilder.ks(k._1))}")
    println(s"dataordered: ${indexOrdered.map(k => rangeBuilder.ks(k._1))}")

    val keysEqual = indexOrdered.map(_._1) == dataOrdered.map(_._1)
    val kvEqualComps = indexOrdered.zipWithIndex.map { case ((k, v, _), idx) =>
      val (k1, v1) = dataOrdered(idx)
      rangeBuilder.ord.equiv(k, k1) && rangeBuilder.ord.equiv(v, v1)
    }

    val kvEqual = kvEqualComps.forall(_ == true)
    val falseOnes = kvEqualComps.filter(_ == false)

    if(!keysEqual){
      falseOnes
      assert(false)
    }

    if(!kvEqual){
      assert(false)
    }

    storage.close()
    //session.close()
  }
}
