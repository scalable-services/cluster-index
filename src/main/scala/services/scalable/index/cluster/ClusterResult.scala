package services.scalable.index.cluster

import services.scalable.index.Tuple

sealed trait ClusterResult {
  val success: Boolean
  val error: Option[Throwable]
}

object ClusterResult {

  case class GetResult[K, V](override val success: Boolean, data: Seq[Tuple[K, V]], override val error: Option[Throwable] = None) extends ClusterResult

  case class InsertionResult(override val success: Boolean, n: Int, override val error: Option[Throwable] = None) extends ClusterResult
  case class UpdateResult(override val success: Boolean, n: Int, override val error: Option[Throwable] = None) extends ClusterResult
  case class RemovalResult(override val success: Boolean, n: Int, override val error: Option[Throwable] = None) extends ClusterResult

  case class BatchResult(override val success: Boolean, error: Option[Throwable] = None, n: Int = 0)
    extends ClusterResult

}
