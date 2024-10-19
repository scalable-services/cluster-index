package services.scalable.index.cluster

import io.netty.util.internal.ThreadLocalRandom
import org.apache.commons.lang3.RandomStringUtils
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory
import services.scalable.index.Commands.{Command, Insert, Remove, Update}
import services.scalable.index.cluster.grpc.KeyIndexContext
import services.scalable.index.grpc.IndexContext
import services.scalable.index.impl.{CassandraStorage, DefaultCache, MemoryStorage}
import services.scalable.index.{DefaultComparators, DefaultSerializers, IndexBuilder, QueryableIndex}

import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class ClusterSpec extends Repeatable with Matchers {

  val logger = LoggerFactory.getLogger(this.getClass)

  override val times: Int = 1

  "operations" should " run successfully" in {

    val rand = ThreadLocalRandom.current()
    import scala.concurrent.ExecutionContext.Implicits.global

    type K = String
    type V = String

    val NUM_LEAF_ENTRIES = 16//rand.nextInt(4, 64)
    val NUM_META_ENTRIES = 16//rand.nextInt(4, 64)

    val clusterIndexId = "cindex1"

    implicit val ord = DefaultComparators.ordString
    val version = "v1"

    //val session = TestHelper.createCassandraSession()
    val storage = /*new CassandraStorage(session, true)*/ new MemoryStorage()

    val MAX_ITEMS = 256//rand.nextInt(128, 512)

    val metaContext = IndexContext()
      .withId(clusterIndexId)
      .withLevels(0)
      .withNumElements(0)
      .withMaxNItems(-1L)
      .withLastChangeVersion(UUID.randomUUID().toString)
      .withNumLeafItems(NUM_LEAF_ENTRIES)
      .withNumMetaItems(NUM_META_ENTRIES)

    val cache = new DefaultCache()

    val rangeBuilder = IndexBuilder.create[K, V](global, DefaultComparators.ordString,
        MAX_ITEMS, MAX_ITEMS, MAX_ITEMS,
        DefaultSerializers.stringSerializer, DefaultSerializers.stringSerializer)
      .storage(storage)
      .cache(cache)
      .serializer(ClusterSerializers.grpcStringStringSerializer)
      .keyToStringConverter(k => k)
      .build()

    val clusterBuilder = IndexBuilder.create[K, KeyIndexContext](global, DefaultComparators.ordString,
        metaContext.numLeafItems, metaContext.numMetaItems, metaContext.maxNItems,
        DefaultSerializers.stringSerializer, ClusterSerializers.keyIndexSerializer)
      .storage(storage)
      .cache(cache)
      .serializer(ClusterSerializers.grpcStringKeyIndexContextSerializer)
      .keyToStringConverter(k => k)
      .build()

    Await.result(storage.loadOrCreate(metaContext), Duration.Inf)

    def remove(data: Seq[(K, V)]): (Boolean, Seq[Command[K, V]]) = {

      if(data.isEmpty) {
        println("no data to remove! Index is empty already!")
        return true -> Seq.empty[Command[K, V]]
      }

      val keys = data.map(_._1)
      var toRemoveRandom = (if(keys.length > 1) scala.util.Random.shuffle(keys).slice(0, rand.nextInt(1, keys.length))
      else keys).map { _ -> Some(version)}

      val removalError = false/*rand.nextInt(1, 100) match {
        case i if i % 7 == 0 =>
          val elem = toRemoveRandom(0)
          toRemoveRandom = toRemoveRandom :+ (elem._1 + "x" , elem._2)
          true

        case _ => false
      }*/

      println(s"${Console.RED_B}REMOVING...${Console.RESET}")
      !removalError -> Seq(Remove(clusterIndexId, toRemoveRandom, Some(version)))
    }

    def insert(data: Seq[(K, V)], upsert: Boolean = false): (Boolean, Seq[Command[K, V]]) = {
      val n = rand.nextInt(100, 1000)
      var list = Seq.empty[(K, V, Boolean)]

      for(i<-0 until n){
        val k = RandomStringUtils.randomAlphabetic(10).toLowerCase()
        val v = k

        if(!data.exists{case (k1, _) => rangeBuilder.ord.equiv(k, k1)} && !list.exists{case (k1, _, _) =>
          rangeBuilder.ord.equiv(k, k1)}){
          list = list :+ (k, v, upsert)
        }
      }

      if(list.isEmpty) {
        println("no unique data to insert!")
        return true -> Seq.empty[Command[K, V]]
      }

      val insertDups = false//rand.nextInt(1, 100) % 7 == 0

      println(s"${Console.GREEN}INSERTING...${Console.RESET}")

      if(insertDups){
        list = list :+ list.head
      }

      !insertDups -> Seq(Insert(clusterIndexId, list, Some(version)))
    }

    def update(data: Seq[(K, V)]): (Boolean, Seq[Command[K, V]]) = {
      if(data.isEmpty) {
        println("no data to update! Index is empty!")
        return true -> Seq.empty[Command[K, V]]
      }

      var toUpdateRandom = (if(data.length > 1) scala.util.Random.shuffle(data).slice(0, rand.nextInt(1, data.length))
      else data).map { case (k, v) => (k, RandomStringUtils.randomAlphabetic(10), Some(version))}

      val updateError = false/*rand.nextInt(1, 100) match {
        case i if i % 13 == 0 =>
          val elem = toUpdateRandom(0)
          toUpdateRandom = toUpdateRandom :+ (elem._1 + "x" , elem._2, elem._3)
          true

        case _ => false
      }*/

      println(s"${Console.BLUE_B}UPDATING...${Console.RESET}")

      !updateError -> Seq(Update(clusterIndexId, toUpdateRandom, Some(version)))
    }

    val runtimes = rand.nextInt(5, 100)
    var data = Seq.empty[(K, V)]

    for(j<-0 until runtimes){

      val ctx = Await.result(storage.loadIndex(clusterIndexId), Duration.Inf).get

      val cindex = new ClusterIndex[K, V](ctx)(rangeBuilder, clusterBuilder)
      var indexData = cindex.inOrderSync().map(x => (x._1, x._2))

      println(s"indexData: ${indexData.length}")

      val nCommands = rand.nextInt(1, 100)
      var cmds = Seq.empty[Command[K, V]]

      for(i<-0 until nCommands){
        cmds ++= (rand.nextInt(1, 10000) match {
          case i if !indexData.isEmpty && i % 3 == 0 =>

            val (ok, cmds) = update(indexData)

            if(ok){
              val list = cmds(0).asInstanceOf[Update[K, V]].list
              indexData = indexData.filterNot{case (k, v) => list.exists{case (k1, _, _) => ord.equiv(k, k1)}}
              indexData = indexData ++ list.map(x => x._1 -> x._2)
            }

            cmds

          case i if !indexData.isEmpty && i % 5 == 0 =>

            val (ok, cmds) = remove(indexData)

            if(ok){
              val list = cmds(0).asInstanceOf[Remove[K, V]].keys
              indexData = indexData.filterNot{case (k, v) => list.exists{case (k1, _) => ord.equiv(k, k1)}}
            }

            cmds

          case _ =>
            val (ok, cmds) = insert(indexData)

            if(ok){
              val list = cmds(0).asInstanceOf[Insert[K, V]].list
              indexData = indexData ++ list.map(x => (x._1, x._2))
            }

            cmds
        })
      }

      val r0 = Await.result(cindex.execute(cmds, version), Duration.Inf)

      if(!r0.success){
        println(r0.error.get.getClass)
        assert(false)
      } else {

        // When everything is ok (all or nothing) executes it...
        for(i<-0 until cmds.length){
          cmds(i) match {
            case cmd: Update[K, V] =>

              val list = cmd.list
              data = data.filterNot{case (k, v) => list.exists{case (k1, _, _) => ord.equiv(k, k1)}}
              data = data ++ list.map(x => x._1 -> x._2)

            case cmd: Remove[K, V] =>

              val list = cmd.keys
              data = data.filterNot{case (k, v) => list.exists{case (k1, _) => ord.equiv(k, k1)}}

            case cmd: Insert[K, V] =>
              val list = cmd.list
              data = data ++ list.map(x => (x._1, x._2))
          }
        }

        Await.result(cindex.save(), Duration.Inf)

        //val indexSortedKeys = cindex.inOrderSync().map{case (k, v, _) => (k, v)}.toList

        println()
      }
    }

    val ctx = Await.result(storage.loadIndex(clusterIndexId), Duration.Inf).get
    val cindex = new ClusterIndex[K, V](ctx)(rangeBuilder, clusterBuilder)

    val indexSortedKeys = cindex.inOrderSync().map{case (k, v, _) => (k, v)}.toList
    val refDataSortedKeys = data.sortBy(_._1).map{case (k, v) => (k, v)}.toList

    if(indexSortedKeys != refDataSortedKeys){
      assert(false)
    } else {
      assert(true)
    }

    Await.result(storage.close(), Duration.Inf)

  }

}
