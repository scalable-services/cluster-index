package cluster

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object MetaWorkerStarter {

  def main(args: Array[String]): Unit = {

    val systems = Seq(new MetaTaskWorker().system)

    Await.result(Future.sequence(systems.map(_.whenTerminated)), Duration.Inf)
  }

}
