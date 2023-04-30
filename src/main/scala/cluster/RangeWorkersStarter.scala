package cluster

import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object RangeWorkersStarter {

  def main(args: Array[String]): Unit = {

    val systems = (0 until 1).map { i =>
      new RangeTaskWorker(s"range-worker-$i").vertx
    }

    //Await.result(Future.sequence(systems.map(_.close().toCompletionStage.toScala)), Duration.Inf)
  }

}
