package cluster

import cluster.ClusterSerializers._
import io.netty.util.internal.ThreadLocalRandom
import org.slf4j.LoggerFactory
import services.scalable.index.impl._
import services.scalable.index.{Bytes, Context, IdGenerator}

import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import services.scalable.index.DefaultPrinters._
import Printers._

object LoadIndexDemo {

  val logger = LoggerFactory.getLogger(this.getClass)

  val indexId = TestConfig.CLUSTER_INDEX_NAME //UUID.randomUUID().toString

  val rand = ThreadLocalRandom.current()

  import scala.concurrent.ExecutionContext.Implicits.global

  type K = Bytes
  type V = Bytes

  import services.scalable.index.DefaultComparators._

  val NUM_LEAF_ENTRIES = 8 //rand.nextInt(5, 64)
  val NUM_META_ENTRIES = 8 //rand.nextInt(5, 64)

  import services.scalable.index.DefaultSerializers._

  implicit val idGenerator = new IdGenerator {
    override def generateId[K, V](ctx: Context[K, V]): String = UUID.randomUUID.toString

    override def generatePartition[K, V](ctx: Context[K, V]): String = "p0"
  }

  implicit val cache = new DefaultCache(MAX_PARENT_ENTRIES = 80000)
  //implicit val storage = new MemoryStorage()
  implicit val storage = new CassandraStorage(TestConfig.session, false)

  def loadAll(): List[String] = {
    val metaContext = Await.result(TestHelper.loadIndex(indexId), Duration.Inf).get

    val cindex = new ClusterIndex[K, V](metaContext, 256,
      NUM_LEAF_ENTRIES,
      NUM_META_ENTRIES)

    val ilist = cindex.inOrder().map { case (k, v, _) => k -> v }.toList.map(_._1).map { k => new String(k) }

    ilist
  }

  def main(args: Array[String]): Unit = {

    val indexIdBefore = s"after-$indexId"

    val ilist = loadAll()

    val ldata = Helper.loadListIndex(indexIdBefore, storage.session).get.keys.map { k =>
      new String(k.toByteArray)
    }.toList

    logger.info(s"${Console.GREEN_B}  ldata (ref) len: ${ldata.length}: ${ldata}${Console.RESET}\n")
    logger.info(s"${Console.MAGENTA_B}idata len:       ${ilist.length}: ${ilist}${Console.RESET}\n")

    println("diff: ", ilist.diff(ldata))
    Await.result(storage.close(), Duration.Inf)

    assert(ldata == ilist)
  }

}
