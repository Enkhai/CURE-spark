package clustering.structures

case class Cluster(points: Array[KDPoint],
                   var representatives: Array[KDPoint],
                   var nearest: Cluster,
                   var mean: KDPoint,
                   var squaredDistance: Double = 0.0d,
                   var id: Int = 0)

