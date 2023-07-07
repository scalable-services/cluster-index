import cluster.grpc.{EAVT, KeyIndexContext}
import com.google.common.primitives.UnsignedBytes
import com.google.protobuf.any.Any
import services.scalable.index.impl.GrpcByteSerializer
import services.scalable.index.{Bytes, Serializer}

package object cluster {

  object Comparators {
    implicit val eavtOrd = new Ordering[EAVT] {
      val comp = UnsignedBytes.lexicographicalComparator()

      override def compare(x: EAVT, y: EAVT): Int = {
        var c = x.e.compare(y.e)

        if(c != 0) return c

        c = x.a.compare(y.a)

        if(c != 0) return c

        c = comp.compare(x.v.toByteArray, y.v.toByteArray)

        if(c != 0) return c

        c = x.valid.compareTo(y.valid)

        if(c != 0) return c

        x.t.compareTo(y.t)
      }
    }
  }

  object Serializers {
    import services.scalable.index.DefaultSerializers._

    implicit val keyIndexSerializer = new Serializer[KeyIndexContext] {
      override def serialize(t: KeyIndexContext): Bytes = Any.pack(t).toByteArray
      override def deserialize(b: Bytes): KeyIndexContext = Any.parseFrom(b).unpack(KeyIndexContext)
    }

    implicit val eavtSerializer = new Serializer[EAVT] {
      override def serialize(t: EAVT): Bytes = Any.pack(t).toByteArray
      override def deserialize(b: Bytes): EAVT = Any.parseFrom(b).unpack(EAVT)
    }

    implicit val grpcByteArrayKeyIndexContextSerializer = new GrpcByteSerializer[Bytes, KeyIndexContext]()

    implicit val grpcEAVTBytesSerializer = new GrpcByteSerializer[EAVT, Bytes]()
  }

  object Printers {
    implicit def keyIndexContextToStringPrinter(k: KeyIndexContext): String = k.toString
    implicit def eavtToStringPrinter(k: EAVT): String = {
     s"(${k.v.toStringUtf8}, ${k.a}, ${k.e}, ${k.t}, ${k.valid})"
    }
  }

}
