package clustering.structures

case class KDTree(var root: KDNode, k: Int) {

  def newNode(point: KDPoint): KDNode =
    KDNode(point, null, null)

  def insert(point: KDPoint): KDNode =
    insertRec(this.root, point, 0)

  def insertRec(node: KDNode, point: KDPoint, depth: Int): KDNode = {
    if (node == null) newNode(point: KDPoint)
    else {
      val axis = depth % k
      if (point.dimensions(axis) < node.point.dimensions(axis))
        node.left = insertRec(node.left, point, depth + 1)
      else if (matchPoints(node.point, point)) {
        node.point.cluster = point.cluster
        node.deleted = false
      }
      else
        node.right = insertRec(node.right, point, depth + 1)
      node
    }
  }

  def matchPoints(point1: KDPoint, point2: KDPoint): Boolean = {
    val d1 = point1.dimensions
    val d2 = point2.dimensions
    !d1.indices.exists(i => d1(i) != d2(i))
  }

  def search(point: KDPoint): Boolean =
    searchRec(this.root, point, 0)

  def searchRec(node: KDNode, point: KDPoint, depth: Int): Boolean = {
    if (node == null)
      return false

    if (matchPoints(node.point, point)) {
      if (!node.deleted)
        true
      else
        false
    } else {
      val axis = depth % k
      if (point.dimensions(axis) < node.point.dimensions(axis))
        searchRec(node.left, point, depth + 1)
      else
        searchRec(node.right, point, depth + 1)
    }
  }

  def delete(point: KDPoint): KDNode =
    deleteRec(this.root, point, 0)

  def deleteRec(node: KDNode, point: KDPoint, depth: Int): KDNode = {
    if (node == null)
      return node

    val axis = depth % k
    if (matchPoints(node.point, point))
      node.deleted = true
    else {
      if (point.dimensions(axis) < node.point.dimensions(axis))
        node.left = deleteRec(node.left, point, depth + 1)
      else
        node.right = deleteRec(node.right, point, depth + 1)
    }

    node
  }

  def copyPoint(p1: KDPoint, p2: KDPoint): Unit = {
    val d1 = p1.dimensions
    val d2 = p2.dimensions
    d1.indices.foreach(i => d1(i) = d2(i))
  }

  def findMin(node: KDNode, d: Int): KDNode =
    findMinRec(node, d, 0)

  def findMinRec(root: KDNode, d: Int, depth: Int): KDNode = {
    if (root == null)
      return null

    val axis = depth % k
    if (axis == d) {
      if (root.left == null)
        root
      else
        findMinRec(root.left, d, depth + 1)
    }
    else
      minNode(root, findMinRec(root.left, d, depth + 1), findMinRec(root.right, d, depth + 1), d)
  }

  def minNode(x: KDNode, y: KDNode, z: KDNode, d: Int): KDNode = {
    var res = x
    if (y != null && y.point.dimensions(d) < res.point.dimensions(d))
      res = y
    if (z != null && z.point.dimensions(d) < res.point.dimensions(d))
      res = z
    res
  }

  def closestPointOfOtherCluster(point: KDPoint): KDPoint = {
    val c = closestRec(this.root, point, 0)
    if (c == null)
      null
    else
      c.point
  }

  def closestRec(node: KDNode, point: KDPoint, depth: Int): KDNode = {
    if (node == null)
      return null
    if (point.cluster == node.point.cluster)
      return closerDistance(point,
        closestRec(node.left, point, depth + 1),
        closestRec(node.right, point, depth + 1))

    val axis = depth % k
    if (point.dimensions(axis) < node.point.dimensions(axis)) {
      val best = {
        if (node.deleted)
          closestRec(node.left, point, depth + 1)
        else
          closerDistance(point,
            closestRec(node.left, point, depth + 1),
            node)
      }
      if (best == null)
        closerDistance(point,
          closestRec(node.right, point, depth + 1),
          best)
      else if (point.squaredDistance(best.point) > Math.pow(point.dimensions(axis) - node.point.dimensions(axis), 2))
        closerDistance(point,
          closestRec(node.right, point, depth + 1),
          best)
      else
        best
    } else {
      val best = {
        if (node.deleted)
          closestRec(node.right, point, depth + 1)
        else
          closerDistance(point,
            closestRec(node.right, point, depth + 1),
            node)
      }
      if (best == null)
        closerDistance(point,
          closestRec(node.left, point, depth + 1),
          best)
      else if (point.squaredDistance(best.point) > Math.pow(point.dimensions(axis) - node.point.dimensions(axis), 2))
        closerDistance(point,
          closestRec(node.left, point, depth + 1),
          best)
      else
        best
    }
  }


  def closerDistance(pivot: KDPoint, n1: KDNode, n2: KDNode): KDNode = {
    if (n1 == null)
      return n2
    if (n2 == null)
      return n1

    val d1 = pivot.squaredDistance(n1.point)
    val d2 = pivot.squaredDistance(n2.point)
    if (d1 < d2) n1
    else n2
  }
}
