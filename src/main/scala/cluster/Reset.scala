package cluster

import com.datastax.oss.driver.api.core.CqlSession
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import services.scalable.index.loader

import scala.jdk.CollectionConverters.{CollectionHasAsScala, SeqHasAsJava}

object Reset {

  def main(args: Array[String]): Unit = {

    val session = CqlSession
      .builder()
      .withConfigLoader(loader)
      .withKeyspace("history")
      .build()

    //println(session.execute("truncate table databases;"))
    println(session.execute("truncate table blocks;"))
    println(session.execute("truncate table indexes;"))
    println(session.execute("truncate table test_indexes;"))

    val config = new java.util.Properties()
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

    val admin = AdminClient.create(config)

    val topics = Seq("meta-index-tasks", "range-index-tasks")

    val existingTopics = admin.listTopics().names().get().asScala.toSeq
    val deleteTopics = existingTopics.filter{t => topics.exists(_ == t)}

    if(!deleteTopics.isEmpty){
      println(admin.deleteTopics(topics.asJava).all().get())
      Thread.sleep(2000)
    }

    admin.createTopics(Seq(new NewTopic("meta-index-tasks", 1, 1.toShort)).asJava)
    admin.createTopics(Seq(new NewTopic("range-index-tasks", 1, 1.toShort)).asJava)

    session.close()
    admin.close()
  }

}
