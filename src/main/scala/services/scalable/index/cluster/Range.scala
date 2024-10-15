package services.scalable.index.cluster

import services.scalable.index.Commands.Command
import services.scalable.index.cluster.ClusterResult.BatchResult
import services.scalable.index.grpc.IndexContext

import scala.concurrent.Future

trait Range[K, V] {

  def save(): Future[IndexContext]
  def execute(commands: Seq[Command[K, V]], version: String): Future[BatchResult]
  def inOrder(): Future[Seq[(K, V, String)]]
  def inOrderSync(): Seq[(K, V, String)]
  def isEmpty(): Future[Boolean]
  def isFull(): Future[Boolean]
  def hasMinimum(): Future[Boolean]
  def hasEnough(): Future[Boolean]
  def borrow(target: Range[K, V]): Future[Boolean]

}
