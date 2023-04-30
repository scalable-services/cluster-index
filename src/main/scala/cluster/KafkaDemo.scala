package cluster

import io.vertx.core.Vertx
import io.vertx.kafka.client.consumer.KafkaConsumer
import org.slf4j.LoggerFactory

object KafkaDemo {

  def main(args: Array[String]): Unit = {

    val logger = LoggerFactory.getLogger(this.getClass)

    val vertx = Vertx.vertx()
    val consumerSettings = new java.util.HashMap[String, String]()

    consumerSettings.put("bootstrap.servers", "localhost:9092")
    consumerSettings.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumerSettings.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    consumerSettings.put("group.id", s"range-task-workers")
    consumerSettings.put("auto.offset.reset", "earliest")
    consumerSettings.put("enable.auto.commit", "false")

    val consumer = KafkaConsumer.create[String, Array[Byte]](vertx, consumerSettings)

    //consumer.batchHandler(handler)
    consumer.handler(m => {

      println("\n\n")
      logger.info("BUCETAAAA")

      consumer.commit().result()
    })

    consumer.subscribe(TestConfig.RANGE_INDEX_TOPIC).result()

  }

}
