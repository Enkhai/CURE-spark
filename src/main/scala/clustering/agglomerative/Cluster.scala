package clustering.agglomerative

class Cluster(point: Array[Double]) {
  var centroid: Array[Double] = point
  var left: Cluster = _
  var right: Cluster = _

  def this(l: Cluster, r: Cluster) {
    this(null)
    left = l
    right = r
    centroid = makeCentroid
  }

  def getPoints: Array[Array[Double]] = {
    if (left == null && right == null)
      Array(centroid)
    else
      left.getPoints ++ right.getPoints
  }

  private def makeCentroid: Array[Double] = {
    getPoints.transpose.map(x => {x.sum / x.length})
  }
}
