import cluster.grpc.{IndexValue, KeyIndexContext}
import com.google.common.primitives.UnsignedBytes
import com.google.protobuf.any.Any
import services.scalable.index.grpc.IndexContext
import services.scalable.index.impl.GrpcByteSerializer
import services.scalable.index.{Bytes, Serializer}

package object cluster {

  object Comparators {

    implicit val indexValueOrd = new Ordering[IndexValue] {
      val comp = UnsignedBytes.lexicographicalComparator()

      override def compare(x: IndexValue, y: IndexValue): Int = {
        var c = comp.compare(x.value.toByteArray, y.value.toByteArray)

        /*if (c != 0) return c

        c = x.valid.compare(y.valid)

        if (c != 0) return c

        x.tmp.compare(y.tmp)*/

        c
      }
    }
  }

  object Serializers {
    import services.scalable.index.DefaultSerializers._

    implicit val metaIndexSerializer = new Serializer[IndexContext] {
      override def serialize(t: IndexContext): Bytes = Any.pack(t).toByteArray
      override def deserialize(b: Bytes): IndexContext = Any.parseFrom(b).unpack(IndexContext)
    }

    implicit val keyIndexSerializer = new Serializer[KeyIndexContext] {
      override def serialize(t: KeyIndexContext): Bytes = Any.pack(t).toByteArray
      override def deserialize(b: Bytes): KeyIndexContext = Any.parseFrom(b).unpack(KeyIndexContext)
    }

    implicit val indexValueSerializer = new Serializer[IndexValue] {
      override def serialize(t: IndexValue): Bytes = Any.pack(t).toByteArray
      override def deserialize(b: Bytes): IndexValue = Any.parseFrom(b).unpack(IndexValue)
    }

    implicit val grpcStringIndexValueBytesSerializer = new GrpcByteSerializer[String, IndexValue]()
    implicit val grpcStringKeyIndexContextSerializer = new GrpcByteSerializer[String, KeyIndexContext]()
  }

  object Printers {
    implicit def keyIndexContextToStringPrinter(k: KeyIndexContext): String = k.toString
    implicit def indexValueToString(k: IndexValue): String = {
     s"(${k.value.toStringUtf8}, ${k.tmp}, ${k.valid})"
    }
  }

}
