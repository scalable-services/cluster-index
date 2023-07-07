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

}
