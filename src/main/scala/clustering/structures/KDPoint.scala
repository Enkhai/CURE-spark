package clustering.structures

class KDPoint(p: Array[Double]) {
  var props: Array[Double] = p

  def get(depth: Int): Double =
    this.props(depth % this.props.length)

  def length: Int =
    this.props.length

  def equals(point: KDPoint): Boolean = {
    if (point.length != this.length)
      return false
    for (i <- 0 until this.length)
      if (point.props(i) != this.props(i))
        return false
    true
  }
}
