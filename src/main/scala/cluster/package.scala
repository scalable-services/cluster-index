import cluster.grpc.KeyIndexContext
import com.google.protobuf.any.Any
import services.scalable.index.grpc.{IndexContext, RootRef}
import services.scalable.index.impl.GrpcByteSerializer
import services.scalable.index.{Bytes, Context, QueryableIndex, Serializer}

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

package object cluster {

  object ClusterSerializers {
    import services.scalable.index.DefaultSerializers._

    implicit val keyIndexSerializer = new Serializer[KeyIndexContext] {
      override def serialize(t: KeyIndexContext): Bytes = Any.pack(t).toByteArray
      override def deserialize(b: Bytes): KeyIndexContext = Any.parseFrom(b).unpack(KeyIndexContext)
    }

    implicit val grpcByteArrayKeyIndexContextSerializer = new GrpcByteSerializer[Bytes, KeyIndexContext]()
  }

  object Printers {
    implicit def keyIndexContextToStringPrinter(k: KeyIndexContext): String = k.toString
  }

  def copy[K, V](source: QueryableIndex[K, V]): QueryableIndex[K, V] = {
    import source._

    val context = IndexContext(UUID.randomUUID.toString, descriptor.numLeafItems,
      descriptor.numMetaItems,
      ctx.root.map { r => RootRef(r._1, r._2) }, levels, ctx.num_elements,
      ctx.maxNItems)

    val copy = new QueryableIndex[K, V](context)(builder)

    ctx.newBlocksReferences.foreach { case (id, b) =>
      copy.ctx.newBlocksReferences += id -> b
    }

    copy
  }

  def split[K, V](source: QueryableIndex[K, V])(implicit ec: ExecutionContext): Future[QueryableIndex[K, V]] = {

    import source._

    for {
      leftR <- ctx.getMeta(ctx.root.get).flatMap {
        case block if block.length == 1 => ctx.getMeta(block.pointers(0)._2.unique_id)
        case block => Future.successful(block)
      }
    } yield {

      val leftN = leftR.pointers.slice(0, leftR.length / 2).map { case (_, ptr) =>
        ptr.nElements
      }.sum

      val rightN = leftR.pointers.slice(leftR.length / 2, leftR.length).map { case (_, ptr) =>
        ptr.nElements
      }.sum

      val leftICtx = descriptor
        .withId(ctx.id)
        .withMaxNItems(descriptor.maxNItems)
        .withNumElements(leftN)
        .withLevels(leftR.level)
        .withNumLeafItems(descriptor.numLeafItems)
        .withNumMetaItems(descriptor.numMetaItems)

      val rightICtx = descriptor
        .withId(UUID.randomUUID().toString)
        .withMaxNItems(descriptor.maxNItems)
        .withNumElements(rightN)
        .withLevels(leftR.level)
        .withNumLeafItems(descriptor.numLeafItems)
        .withNumMetaItems(descriptor.numMetaItems)

      val refs = ctx.newBlocksReferences

      ctx = Context.fromIndexContext(leftICtx)(builder)

      ctx.newBlocksReferences ++= refs

      val rindex = new QueryableIndex[K, V](rightICtx)(builder)

      val leftRoot = leftR.copy()(ctx)
      ctx.root = Some(leftRoot.unique_id)

      val rightRoot = leftRoot.split()(rindex.ctx)
      rindex.ctx.root = Some(rightRoot.unique_id)

      /*val ileft = Await.result(TestHelper.all(left.inOrder()), Duration.Inf).map { case (k, v, _) => k -> v }
        logger.debug(s"${Console.BLUE_B}idata data: ${ileft.map { case (k, v) => new String(k, Charsets.UTF_8) -> new String(v) }}${Console.RESET}\n")

        val idataL = Await.result(TestHelper.all(lindex.inOrder()), Duration.Inf).map { case (k, v, _) => k -> v }
        logger.debug(s"${Console.GREEN_B}idataL data: ${idataL.map { case (k, v) => new String(k, Charsets.UTF_8) -> new String(v) }}${Console.RESET}\n")

        val idataR = Await.result(TestHelper.all(rindex.inOrder()), Duration.Inf).map { case (k, v, _) => k -> v }
        logger.debug(s"${Console.GREEN_B}idataR data: ${idataR.map { case (k, v) => new String(k, Charsets.UTF_8) -> new String(v) }}${Console.RESET}\n")

        assert(idataR.slice(0, idataL.length) != idataL)
        assert(ileft == (idataL ++ idataR))*/

      rindex
    }
  }

}
