package clustering.agglomerative__

import clustering.Distance.euclideanDistance

class AgglomerativeClustering__(numClusters: Int) {

  validateArgs()

  // Implements Average-Linkage technique
  def fitPredict(X: Array[Array[Double]]): Array[(Cluster__, Int)] = {
    // Create an ArrayBuffer containing a cluster for each point
    val clusters = X.map(x => new Cluster__(x)).toBuffer

    // Calculate closest points and closest cluster distances for each point (cluster)
    // Cluster closest and closest distances are initially null and we have to make them
    for (i <- clusters.indices) {
      var closestDist = Double.MaxValue
      var closest: Cluster__ = null
      for (j <- i + 1 until(clusters.length)) {
        val currDist = euclideanDistance(clusters(i).centroid, clusters(j).centroid)
        if (currDist < closestDist) {
          closestDist = currDist
          closest = clusters(j)
        }
      }
      clusters(i).closest = closest
      clusters(i).closestDistance = closestDist
    }

    // Repeat while the number of clusters do not satisfy the specified number
    while (clusters.length > numClusters) {
      // Get the left node of the new cluster, specified by the minimum closest cluster distance in the buffer
      val left = clusters.minBy(_.closestDistance)
      // Get the right node accordingly
      val right = left.closest

      // Create the new cluster and drop the old ones from the buffer
      val newCluster = new Cluster__(left, right)
      clusters -= (left, right)

      // Find the closest cluster and closest cluster distance for the new cluster
      var newClosestDist = Double.MaxValue
      var newClosest: Cluster__ = null
      for (i <- clusters.indices) {
        val newCurrDist = euclideanDistance(clusters(i).centroid, newCluster.centroid)
        if (newCurrDist < newClosestDist) {
          newClosestDist = newCurrDist
          newClosest = clusters(i)
        }
        // If the distance found between the new and an existing cluster
        // is less than the existing cluster's closest distance
        // replace the closest distance and the closest cluster
        if (newCurrDist < clusters(i).closestDistance){
          clusters(i).closest = newCluster
          clusters(i).closestDistance = newCurrDist
        }
      }
      newCluster.closest = newClosest
      newCluster.closestDistance = newClosestDist

      clusters.append(newCluster)
    }

    clusters.zipWithIndex.toArray
  }

  private def validateArgs(): Unit = {
    if (numClusters < 1)
      throw new Exception("Please specify more than one cluster")
  }
}
