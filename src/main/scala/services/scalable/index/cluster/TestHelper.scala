package services.scalable.index.cluster

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.config.{DefaultDriverOption, DriverConfigLoader}
import TestConfig._

object TestHelper {

  def createCassandraSession(): CqlSession = {
    val loader =
      DriverConfigLoader.programmaticBuilder()
        .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, java.time.Duration.ofSeconds(30))
        .withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, 31768)
        .withInt(DefaultDriverOption.SESSION_LEAK_THRESHOLD, 1000)
        .withString(DefaultDriverOption.PROTOCOL_VERSION, "V4")
        .withString(DefaultDriverOption.RECONNECTION_POLICY_CLASS, "ExponentialReconnectionPolicy")
        .withDuration(DefaultDriverOption.RECONNECTION_BASE_DELAY, java.time.Duration.ofSeconds(1))
        .withDuration(DefaultDriverOption.RECONNECTION_MAX_DELAY, java.time.Duration.ofSeconds(10))
        /*.startProfile("slow")
        .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
        .endProfile()*/
        .build()

    CqlSession
      .builder()
      .withKeyspace(KEYSPACE)
      .withAuthCredentials(CQL_USER, CQL_PWD)
      .withConfigLoader(loader)
      .build()
  }

}
