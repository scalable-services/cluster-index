package cluster

import com.google.common.base.Charsets
import io.netty.util.internal.ThreadLocalRandom
import org.apache.commons.lang3.RandomStringUtils
import org.slf4j.LoggerFactory
import services.scalable.index.grpc.IndexContext
import services.scalable.index.impl._
import services.scalable.index.{Bytes, Commands, Context, IdGenerator, Leaf, Meta, QueryableIndex, Tuple}

import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class IndexSplittingSpec extends Repeatable {

  override val times: Int = 1

  "operations" should " run successfully" in {

    val logger = LoggerFactory.getLogger(this.getClass)

    val rand = ThreadLocalRandom.current()
    import scala.concurrent.ExecutionContext.Implicits.global

    type K = Bytes
    type V = Bytes

    import services.scalable.index.DefaultComparators._
    import services.scalable.index.DefaultSerializers._

    val NUM_LEAF_ENTRIES = 4//rand.nextInt(4, 64)
    val NUM_META_ENTRIES = 4//rand.nextInt(4, 64)

    val indexId = UUID.randomUUID().toString

    implicit val idGenerator = new IdGenerator {
      override def generateId[K, V](ctx: Context[K, V]): String = UUID.randomUUID.toString
      override def generatePartition[K, V](ctx: Context[K, V]): String = "p0"
    }

    implicit val cache = new DefaultCache(MAX_PARENT_ENTRIES = 80000)
    implicit val storage = new MemoryStorage(NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
    //implicit val storage = new CassandraStorage(TestConfig.KEYSPACE, NUM_LEAF_ENTRIES, NUM_META_ENTRIES, false)

    val indexContext = Await.result(storage.loadOrCreateIndex(indexId, NUM_LEAF_ENTRIES, NUM_META_ENTRIES), Duration.Inf)

    var data = Seq.empty[(K, V)]
    val index = new QueryableIndex[K, V](indexContext)

    def insert(): Unit = {
      val n = 2000//rand.nextInt(1, 1000)
      var list = Seq.empty[Tuple[K, V]]

      for(i<-0 until n){
        val k = RandomStringUtils.randomAlphanumeric(5, 10).getBytes(Charsets.UTF_8)
        val v = RandomStringUtils.randomAlphanumeric(5).getBytes(Charsets.UTF_8)

        if(!data.exists{case (k1, _) => ord.equiv(k, k1)}){
          list = list :+ (k -> v)
        }
      }

      val cmds = Seq(
        Commands.Insert(indexId, list)
      )
      val result = Await.result(index.execute(cmds), Duration.Inf)

      index.snapshot()

      if(result){
        data = data ++ list
      }
    }

    insert()
    insert()

    logger.info(Await.result(index.save(), Duration.Inf).toString)

    val level = index.ctx.levels

    val list = Await.result(TestHelper.all(index.inOrder()), Duration.Inf)
    logger.debug(s"${Console.CYAN_B}index: ${list.map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v)}}${Console.RESET}\n")

    val leftRootBlock = Await.result(index.ctx.get(index.ctx.root.get), Duration.Inf)//.asInstanceOf[Meta[K, V]]

    leftRootBlock match {
      case leaf: Leaf[K, V] => println(leaf.inOrder().map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v)})
      case meta: Meta[K, V] => println(meta.inOrder().map{case (k, v) => new String(k, Charsets.UTF_8) -> v})
    }
    println()

    println(s"left len: ${leftRootBlock.length}\n")

    val right = new QueryableIndex[K, V](IndexContext("main"))
    val rightRootBlock = leftRootBlock.split()(right.ctx)

    val leftPointer = leftRootBlock match {
      case leaf: Leaf[K, V] => leaf.unique_id
      case meta: Meta[K, V] => if(meta.length == 1) meta.pointers(0)._2.unique_id else meta.unique_id
    }

    val rightPointer = rightRootBlock match {
      case leaf: Leaf[K, V] => leaf.unique_id
      case meta: Meta[K, V] => if(meta.length == 1) meta.pointers(0)._2.unique_id else meta.unique_id
    }

    println(leftPointer, leftRootBlock.unique_id, rightPointer, rightRootBlock.unique_id)

    val leftLink = Await.result(index.ctx.get(leftPointer), Duration.Inf)
    val rightLink = Await.result(right.ctx.get(rightPointer), Duration.Inf)

    val left = new QueryableIndex[K, V](IndexContext("main"))
    left.ctx.root = Some(leftPointer)
    left.ctx.num_elements = leftLink.nSubtree
    left.ctx.levels = leftLink.level

    right.ctx.root = Some(rightPointer)
    right.ctx.num_elements = rightLink.nSubtree
    right.ctx.levels = rightLink.level

    val leftList = Await.result(TestHelper.all(left.inOrder()), Duration.Inf)
    val rightList = Await.result(TestHelper.all(right.inOrder()), Duration.Inf)

    logger.debug(s"${Console.GREEN_B}left: ${leftList.map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v)}}${Console.RESET}\n")
    logger.debug(s"${Console.MAGENTA_B}right: ${rightList.map{case (k, v) => new String(k, Charsets.UTF_8) -> new String(v)}}${Console.RESET}\n")

    println()
    println("left n: ", leftLink.nSubtree, "left level: ", leftLink.level)
    println("right n: ", rightLink.nSubtree, "right level: ", rightLink.level)
    println("n: ", list.length, "original levels", level)
    println()

    assert(list.length == leftLink.nSubtree + rightLink.nSubtree)

    assert(TestHelper.isColEqual(list, leftList ++ rightList))
  }

}
