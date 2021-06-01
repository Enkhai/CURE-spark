package clustering.hierarchical

object AgglomerativeAlgorithm {

  def start(initialClusters: Array[Array[Array[Double]]], k: Int): Array[(Array[Double], Int)] = {

    var clusters = initialClusters.map(x => (x, new Array[Double](x(0).length)))
      .toBuffer

    while (clusters.length > k) {

      clusters = clusters.map(x => (x._1, meanOfPoints(x._1)))

      val distances = clusters.map(x => clusters.map(y => squaredDistance(y._2, x._2)).zipWithIndex.toArray)
      val minDistances = distances.map(x => x.filter(_._1 != 0).minBy(_._1))
      val min = minDistances.zipWithIndex.minBy(x => x._1._1)

      val newClusterPoints = clusters(min._1._2)._1 ++ clusters(min._2)._1
      val newCluster = (newClusterPoints, meanOfPoints(newClusterPoints))

      clusters.remove(min._1._2)
      clusters.remove(min._2)

      clusters.append(newCluster)
    }

    clusters.zipWithIndex
      .flatMap(x => x._1._1.map(y => (y, x._2)))
      .toArray
  }

  def meanOfPoints(points: Array[Array[Double]]): Array[Double] = {
    points.transpose.
      map(x => x.sum / x.length)
  }

  def squaredDistance(p1: Array[Double], p2: Array[Double]): Double = {
    p1.indices.foldLeft(0.0d) { (l, r) => l + Math.pow(p1(r) - p2(r), 2) }
  }
}
