package services.scalable.index

import com.google.protobuf.any.Any
import services.scalable.index.cluster.grpc.{BankAccount, KeyIndexContext}
import services.scalable.index.impl.{GrpcByteSerializer, GrpcCommandSerializer}

package object cluster {

  object ClusterSerializers {

    import services.scalable.index.DefaultSerializers._

    implicit val bankAccountSerializer = new Serializer[BankAccount] {
      override def serialize(t: BankAccount): Bytes = BankAccount.toByteArray(t)
      override def deserialize(b: Bytes): BankAccount = BankAccount.parseFrom(b)
    }

    implicit val keyIndexSerializer = new Serializer[KeyIndexContext] {
      override def serialize(t: KeyIndexContext): Bytes = Any.pack(t).toByteArray

      override def deserialize(b: Bytes): KeyIndexContext = Any.parseFrom(b).unpack(KeyIndexContext)
    }

    implicit val grpcStringStringSerializer = new GrpcByteSerializer[String, String]()
    implicit val grpcStringBankAccountSerializer = new GrpcByteSerializer[String, BankAccount]()

    implicit val grpcStringKeyIndexContextSerializer = new GrpcByteSerializer[String, KeyIndexContext]()

    implicit val grpcStringStringCommandsSerializer = new GrpcCommandSerializer[String, String]()
    implicit val grpcStringKeyIndexContextCommandsSerializer = new GrpcCommandSerializer[String, KeyIndexContext]()
    //implicit val grpcRangeCommandSerializer = new GrpcRangeCommandSerializer[String, String]()
    //implicit val grpcMetaCommandSerializer = new GrpcMetaCommandSerializer[String]()

    implicit val grpcIntBytesSerializer: GrpcByteSerializer[Int, Bytes] = new GrpcByteSerializer[Int, Bytes]()
  }

  object ClusterPrinters {
    implicit def keyIndexContextToStringPrinter(k: KeyIndexContext): String = k.toString
    implicit def intToStringPrinter(k: Int): String = k.toString
  }

}
