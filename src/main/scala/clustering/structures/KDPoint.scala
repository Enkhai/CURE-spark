package clustering.structures

case class KDPoint(dimensions: Array[Double],
                   var cluster: Cluster = null) {

  def distance(p: KDPoint): Double = {
    Math.sqrt(squaredDistance(p))
  }

  def squaredDistance(p: KDPoint): Double = {
    val d1 = this.dimensions
    val d2 = p.dimensions
    d1.indices.foldLeft(0.0d) { (l, r) => l + Math.pow(d1(r) - d2(r), 2) }
  }

  override def equals(other: scala.Any): Boolean = {
    !other
      .asInstanceOf[KDPoint]
      .dimensions
      .indices
      .exists(i => this.dimensions(i) != other.asInstanceOf[KDPoint].dimensions(i))
  }

  override def toString: String =
    dimensions.toList.toString()
}

