package clustering.hierarchical

import clustering.structures._

object AgglomerativeAlgorithm {

  def start(initialClusters: Array[Cluster], k: Int): Array[(Array[Double], Int)] = {
    val tree = createTree(initialClusters.flatMap(_.points))
    val heap = createHeap(initialClusters, tree)

    while (heap.heapSize > k) {
      val c1 = heap.takeHead()
      val nearest = c1.nearest
      val c2 = merge(c1, nearest)

      val (newNearestCluster, nearestDistance) = getNearestCluster(c2, tree)
      c2.nearest = newNearestCluster
      c2.squaredDistance = nearestDistance

      removeClustersFromHeap(tree, heap, c1, nearest)

      heap.insert(c2)
      if (heap.heapSize % 20 == 0) println(s"${heap.heapSize} clusters remaining.")
    }

    heap.getDataArray
      .slice(0, heap.heapSize)
      .zipWithIndex
      .flatMap(c => c._1.points.map(p => (p.dimensions, c._2)))
  }

  private def createTree(points: Array[KDPoint]): KDTree = {
    val tree = KDTree(KDNode(points.head, null, null), points.head.dimensions.length)
    for (i <- 1 until points.length)
      tree.insert(points(i))
    tree
  }

  private def createHeap(clusters: Array[Cluster], tree: KDTree): MinHeap = {
    val heap = MinHeap(clusters.length)
    clusters.foreach(c => {
      val (nearestCluster, nearestDistance) = getNearestCluster(c, tree)
      c.nearest = nearestCluster
      c.squaredDistance = nearestDistance
      heap.insert(c)
    })
    heap
  }

  private def getNearestCluster(cluster: Cluster, tree: KDTree): (Cluster, Double) = {
    val (nearestRep, nearestDistance) = cluster
      .points
      .foldLeft(null: KDPoint, Double.MaxValue) {
        case ((currNearestPoint, currNearestDistance), point) =>
          val nearestPoint = tree.closestPointOfOtherCluster(point)
          val nearestDistance = point.squaredDistance(nearestPoint)
          if (nearestDistance < currNearestDistance)
            (nearestPoint, nearestDistance)
          else
            (currNearestPoint, currNearestDistance)
      }
    (nearestRep.cluster, nearestDistance)
  }

  def merge(c1: Cluster, nearest: Cluster): Cluster = {
    val mergedPoints = c1.points ++ nearest.points
    val newCluster = Cluster(mergedPoints, null, null, null)
    mergedPoints.foreach(_.cluster = newCluster)
    newCluster
  }

  private def removeClustersFromHeap(kdTree: KDTree, cHeap: MinHeap, cluster: Cluster, nearest: Cluster): Unit = {
    val heapSize = cHeap.heapSize
    var i = 0
    while (i < heapSize) {
      var continue = true
      val currCluster = cHeap.getDataArray(i)
      val currNearest = currCluster.nearest
      if (currCluster == nearest) {
        cHeap.remove(i)
        continue = false
      }
      if (currNearest == nearest || currNearest == cluster) {
        val (newCluster, newDistance) = getNearestCluster(currCluster, kdTree)
        currCluster.nearest = newCluster
        currCluster.squaredDistance = newDistance
        cHeap.heapify(i)
        continue = false
      }
      if (continue) i += 1
    }
  }
}
