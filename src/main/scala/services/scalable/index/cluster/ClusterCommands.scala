package services.scalable.index.cluster

import services.scalable.index.Commands
import services.scalable.index.cluster.grpc.KeyIndexContext

object ClusterCommands {
  trait ClusterCommand[K, V] {
    val id: String
  }

  case class RangeCommand[K, V](override val id: String,
                                rangeId: String,
                                indexId: String,
                                commands: Seq[Commands.Command[K, V]],
                                keyInMeta: Tuple2[K, String],
                                lastChangeVersion: String
                               ) extends ClusterCommand[K, V]
  case class MetaCommand[K](override val id: String,
                            metaId: String,
                            commands: Seq[Commands.Command[K, KeyIndexContext]]
                           ) extends ClusterCommand[K, KeyIndexContext]
}
