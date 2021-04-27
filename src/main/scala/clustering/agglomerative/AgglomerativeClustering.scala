package clustering.agglomerative

import clustering.Misc.euclideanDistance

class AgglomerativeClustering(numClusters: Int) {

  validateArgs()

  def fitPredict(X: Array[Array[Double]]): Array[(Cluster, Int)] = {
    val clusters = X.map(x => new Cluster(x)).toBuffer

    while (clusters.length > numClusters) {
      var min_dist = Double.MaxValue
      var min_i = 0
      var min_j = 1
      for (i <- clusters.indices) {
        for (j <- i + 1 until clusters.length) {
          val curr_dist = euclideanDistance(clusters(i).centroid, clusters(j).centroid)
          if(curr_dist < min_dist) {
            min_dist = curr_dist
            min_i = i
            min_j = j
          }
        }
      }
      clusters.append(new Cluster(clusters(min_i), clusters(min_j)))
      clusters.remove(min_i)
      clusters.remove(min_j)
    }
    clusters.zipWithIndex.toArray
  }

  private def validateArgs(): Unit = {
    if (numClusters < 1)
      throw new Exception("Please specify more than one cluster")
  }
}
