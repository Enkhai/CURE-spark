package clustering.structures

import scala.collection.mutable.ArrayBuffer

class Cluster(point: Array[Double], c: Int) {
  var points: ArrayBuffer[Array[Double]] = ArrayBuffer[Array[Double]](point)
  var centroid: Array[Double] = point
  var representatives: Array[Array[Double]] = new Array[Array[Double]](c)
  representatives(0) = point
  var closest: Cluster = _
  var closestDistance: Double = Double.MaxValue

  def merge(b:Cluster): Cluster = {

    // TODO: Implement merge

  }
}
