package clustering.structures

case class KDNode(point: KDPoint,
                  var left: KDNode,
                  var right: KDNode,
                  var deleted: Boolean = false)
