package services.scalable.index.cluster

object TestConfig {

  val KEYSPACE = "history"
  val CQL_USER = "cassandra"
  val CQL_PWD = "cassandra"
  val DATABASE = "myHistoryIndex"

  val NUM_LEAF_ENTRIES = 16
  val NUM_META_ENTRIES = 16
  val MAX_ITEMS = 100


}
