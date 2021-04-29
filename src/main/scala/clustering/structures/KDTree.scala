package clustering.structures

import clustering.Distance.euclideanDistanceSquared

class KDTree {

  var root: KDNode = _
  var numDims: Int = _

  def this(numDims: Int) {
    this
    this.numDims = numDims
  }

  def this(root: KDNode) {
    this
    this.root = root
    this.numDims = root.point.length
  }

  def this(points: Array[Array[Double]]) {
    this
    this.numDims = points(0).length
    this.root = new KDNode(points(0))

    for (i <- 1 until points.length) {
      this.root.add(new KDNode(points(i)))
    }
  }

  def add(props: Array[Double]): Unit =
    add(new KDNode(props))

  def add(node: KDNode): Unit = {
    if (this.root == null)
      this.root = node
    else
      root.add(node)
  }

  def remove(props: Array[Double]): Unit = {
    remove(new KDPoint(props))
  }

  def remove(point: KDPoint): Unit = {
    this.root = remove(this.root, point, 0)
  }

  def remove(root: KDNode, point: KDPoint, depth: Int): KDNode = {
    if (root == null)
      return null

    val currDim = depth % this.numDims

    if (root.point.equals(point)) {
      if (root.right != null) {
        val min = findMin(root.right, currDim)
        root.point = min.point
        root.right = remove(root.right, min.point, depth + 1)
      } else if (root.left != null) {
        val min = findMin(root.left, currDim)
        root.point = min.point
        root.right = remove(root.left, min.point, depth + 1)
      } else
        return null
      return root
    }

    if (point.props(depth) < root.point.props(depth))
      root.left = remove(root.left, point, depth + 1)
    else
      root.right = remove(root.right, point, depth + 1)

    root
  }

  def findMin(root: KDNode, dim: Int): KDNode = {
    findMin(root, dim, 0)
  }

  def findMin(root: KDNode, dim: Int, depth: Int): KDNode = {
    if (root == null)
      return null

    val currDim = depth % this.numDims
    if (currDim == dim) {
      if (root.left == null)
        return root
      return findMin(root.left, dim, depth + 1)
    }

    minNode(root, findMin(root.left, dim, depth + 1), findMin(root.right, dim, depth + 1), dim)
  }

  def minNode(x: KDNode, y: KDNode, z: KDNode, dim: Int): KDNode = {
    var min = x
    if (y != null && y.point.props(dim) < min.point.props(dim))
      min = y
    if (z != null && z.point.props(dim) < min.point.props(dim))
      min = z
    min
  }

  def nearestNeighbor(props: Array[Double]): KDNode =
    nearestNeighbor(new KDPoint(props))

  def nearestNeighbor(target: KDPoint): KDNode =
    nearestNeighbor(this.root, target, 0)

  private def nearestNeighbor(root: KDNode, target: KDPoint, depth: Int): KDNode = {
    if (root == null)
      return null

    var nextBranch: KDNode = null
    var otherBranch: KDNode = null

    if (target.get(depth) < root.point.get(depth)) {
      nextBranch = root.left
      otherBranch = root.right
    } else {
      nextBranch = root.right
      otherBranch = root.left
    }

    var temp = nearestNeighbor(nextBranch, target, depth + 1)
    var best = closest(temp, root, target)

    val radiusSquared = euclideanDistanceSquared(target.props, best.point.props)
    val dist = target.get(depth) - root.point.get(depth)

    if (radiusSquared >= dist * dist) {
      temp = nearestNeighbor(otherBranch, target, depth + 1)
      best = closest(temp, best, target)
    }

    best
  }

  def closest(n0: KDNode, n1: KDNode, target: KDPoint): KDNode = {
    if (n0 == null) return n1
    if (n1 == null) return n0

    if (euclideanDistanceSquared(n0.point.props, target.props) < euclideanDistanceSquared(n1.point.props, target.props))
      n0
    else
      n1
  }

}
