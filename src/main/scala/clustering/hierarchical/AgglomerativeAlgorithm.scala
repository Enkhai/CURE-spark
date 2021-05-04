package clustering.hierarchical

import clustering.structures.{Cluster, KDTree, MinHeap}

class AgglomerativeAlgorithm {

  def start(initialClusters: List[Cluster]): Unit = {
    val heap = MinHeap(initialClusters.length)
    // val tree = KDTree()

    // TODO: Implement this

  }
}
