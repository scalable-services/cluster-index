package services.scalable.index.cluster

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.config.{DefaultDriverOption, DriverConfigLoader}

import scala.concurrent.ExecutionContext

object TestHelper {

  val TX_VERSION = "v1"
  val CLUSTER_INDEX_NAME = "index"
  val MAX_LEAF_ITEMS = 32
  val MAX_META_ITEMS = 32
  val MAX_RANGE_ITEMS = 512L

  val KEYSPACE = "history"

  val loader =
    DriverConfigLoader.programmaticBuilder()
      .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, java.time.Duration.ofSeconds(30))
      .withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, 31768)
      .withInt(DefaultDriverOption.SESSION_LEAK_THRESHOLD, 10000)
      .withString(DefaultDriverOption.PROTOCOL_VERSION, "V4")
      .withString(DefaultDriverOption.RECONNECTION_POLICY_CLASS, "ExponentialReconnectionPolicy")
      .withDuration(DefaultDriverOption.RECONNECTION_BASE_DELAY, java.time.Duration.ofSeconds(1))
      .withDuration(DefaultDriverOption.RECONNECTION_MAX_DELAY, java.time.Duration.ofSeconds(10))
      /*.startProfile("slow")
      .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
      .endProfile()*/
      .build()

  def truncateAll()(implicit session: CqlSession, ec: ExecutionContext): Unit = {
    println("truncate ranges: ", session.execute("TRUNCATE TABLE ranges;").wasApplied())
    println("truncate indexes: ", session.execute("TRUNCATE TABLE indexes;").wasApplied())
    println("truncate indexes: ", session.execute("TRUNCATE TABLE blocks;").wasApplied())
  }

  def getSession(): CqlSession = {
    CqlSession
      .builder()
      //.withLocalDatacenter("datacenter1")
      .withConfigLoader(loader)
      .withKeyspace(KEYSPACE)
      //.withAuthCredentials(CQL_USER, CQL_PWD)
      .build()
  }

}
