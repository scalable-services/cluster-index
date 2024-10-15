package services.scalable.index.cluster

import services.scalable.index.IndexBuilt
import services.scalable.index.grpc.IndexContext

final class ClusterClient[K, V](val descriptor: IndexContext)
                               (implicit val rangeBuilder: IndexBuilt[K, V]) {

  assert(rangeBuilder.MAX_N_ITEMS > 0)
  assert(rangeBuilder.MAX_META_ITEMS > 0)
  assert(rangeBuilder.MAX_LEAF_ITEMS > 0)

}
