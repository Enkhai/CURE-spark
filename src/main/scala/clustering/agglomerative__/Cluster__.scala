package clustering.agglomerative__

class Cluster__(point: Array[Double]) {
  var centroid: Array[Double] = point
  var left: Cluster__ = _
  var right: Cluster__ = _
  var closest: Cluster__ = _
  var closestDistance: Double = _

  def this(l: Cluster__, r: Cluster__) {
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
