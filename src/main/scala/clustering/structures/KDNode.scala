package clustering.structures

class KDNode {
  var left: KDNode = _
  var right: KDNode = _
  var numDims: Int = _
  var point: KDPoint = _

  def this(props: Array[Double]) {
    this
    this.point = new KDPoint(props)
    this.numDims = props.length
  }

  def this(point: KDPoint) {
    this
    this.point = point
    this.numDims = point.length
  }

  def add(n: KDNode): Unit =
    this.add(n, 0)

  def add(n: KDNode, k: Int): Unit = {
    if (n.point.get(k) < this.point.get(k)) {
      if (this.left == null)
        this.left = n
      else
        this.left.add(n, k + 1)
    } else {
      if (this.right == null)
        this.right = n
      else
        this.right.add(n, k + 1)
    }
  }

  def equals(node: KDNode): Boolean =
    this.point.equals(node.point)
}
