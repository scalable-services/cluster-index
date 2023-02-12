package cluster

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object Workers {

  def main(args: Array[String]): Unit = {

    val systems = (0 until 1).map { i =>
      new RangeTaskWorker(s"range-worker-$i").system
    }

    Await.result(Future.sequence(systems.map(_.whenTerminated)), Duration.Inf)
  }

}
