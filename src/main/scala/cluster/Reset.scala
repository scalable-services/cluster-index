package cluster

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import scala.jdk.CollectionConverters.{CollectionHasAsScala, SeqHasAsJava}

object Reset {

  def main(args: Array[String]): Unit = {

    import TestConfig.session

    println(session.execute("truncate table blocks;").wasApplied())
    println(session.execute("truncate table indexes;").wasApplied())
    println(session.execute("truncate table test_indexes;").wasApplied())

    session.close()

    val config = new java.util.Properties()
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

    val admin = AdminClient.create(config)

    val topics = Seq(TestConfig.RANGE_INDEX_TOPIC, TestConfig.META_INDEX_TOPIC)

    val existingTopics = admin.listTopics().names().get().asScala.toSeq
    val deleteTopics = existingTopics.filter{t => topics.exists(_ == t)}

    if(!deleteTopics.isEmpty){
      println(admin.deleteTopics(topics.asJava).all().get())
      Thread.sleep(2000L)
    }

    admin.createTopics(Seq(new NewTopic(TestConfig.RANGE_INDEX_TOPIC, 1, 1.toShort)).asJava).all().get()
    admin.createTopics(Seq(new NewTopic(TestConfig.META_INDEX_TOPIC, 1, 1.toShort)).asJava).all().get()

    admin.close()
  }

}
