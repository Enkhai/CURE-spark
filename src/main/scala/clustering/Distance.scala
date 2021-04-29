package clustering

import clustering.structures.Cluster

object Distance {

  def euclideanDistance(x: Array[Double], y: Array[Double]): Double = {
    Math.sqrt(euclideanDistanceSquared(x, y))
  }

  def euclideanDistanceSquared(x: Array[Double], y: Array[Double]): Double = {
    (x zip y).map { case (i, j) => Math.pow(i - j, 2) }.sum
  }

  def clusterDistance(x: Cluster, y: Cluster): Double = {
    var minDistance = Double.MaxValue
    for (i <- x.points) {
      for (j <-y.points) {
        val currDist = euclideanDistanceSquared(i, j)
        if (currDist < minDistance)
          minDistance = currDist
      }
    }
    minDistance
  }
}
