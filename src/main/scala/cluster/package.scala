package object cluster {

  val EMPTY_ARRAY = Array.empty[Byte]

  type Bytes = Array[Byte]

  type Pointer = Tuple2[Bytes, String]
  type Tuple = Tuple2[Bytes, Bytes]

}
