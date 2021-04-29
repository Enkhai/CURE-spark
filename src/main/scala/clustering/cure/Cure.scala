package clustering.cure

import clustering.structures.{Cluster, KDTree, MinHeap}
import clustering.Distance.{clusterDistance, euclideanDistanceSquared}
import org.apache.spark.rdd.RDD

class Cure(s: Int, k: Int, c: Int, a: Double) {

  validateArgs()

  def fitPredict(rdd: RDD[Array[Double]]): RDD[(Array[Double], Int)] = {
    val sample = rdd.takeSample(withReplacement = false, s)

    // TODO: Implement this

  }

  def cluster(S: Array[Array[Double]]): Unit = {
    val tree = new KDTree(S)
    val heap = buildHeap(S)
    while (heap.size > k) {
      val u = heap.extractMin()
      val uClosest = u.closest
      heap.delete(uClosest)
      val w = u.merge(uClosest)
      deleteRepresentatives(tree, u)
      deleteRepresentatives(tree, uClosest)
      insertRepresentatives(tree, w)
      w.closest = heap.getMin
      for (x <- heap.heap) {
        val xwDistance = clusterDistance(x, w)
        if (xwDistance < clusterDistance(w, w.closest))
          w.closest = x
        if (x.closest == u || x.closest == uClosest){
          if (x.closestDistance < xwDistance) {
            x.closest = closetCluster(tree, x, xwDistance)
            x.closestDistance = clusterDistance(x.closest, x)
          } else {
            x.closest = w
            x.closestDistance = xwDistance
          }
          heap.relocate(x)
        }
      }
      heap.insert(w)
    }

  }

  def closetCluster(tree: KDTree, cluster: Cluster, d: Double): Cluster = {
    // TODO: Implement this
  }

  def deleteRepresentatives(tree: KDTree, cluster: Cluster): Unit = {
    for (r <- cluster.representatives)
      tree.remove(r)
  }

  def insertRepresentatives(tree: KDTree, cluster: Cluster): Unit = {
    for (r <- cluster.representatives)
      tree.add(r)
  }

  def buildHeap(points: Array[Array[Double]]): MinHeap = {
    val clusters = points.map(x => new Cluster(x, c))
    for (i <- clusters.indices) {
      var closestDist = Double.MaxValue
      var closest: Cluster = null
      for (j <- i until clusters.length) {
        val currDistance = euclideanDistanceSquared(clusters(i).centroid, clusters(i).centroid)
        if (currDistance > clusters(i).closestDistance) {
          closestDist = currDistance
          closest = clusters(j)
        }
      }
      clusters(i).closest = closest
      clusters(i).closestDistance = closestDist
    }
    val heap = new MinHeap()
    for (cluster <- clusters)
      heap.insert(cluster)
    heap
  }

  private def validateArgs() {
    if (500 < s && s > 5000)
      throw new Exception("Sample size must be between 500 and 5,000.")
    if (k < 1)
      throw new Exception("Please specify more than two clusters.")
    if (c <= 1)
      throw new Exception("Please specify one or more representatives for a cluster.")
    if (0 > a || a > 1)
      throw new Exception("Attribute shrinking factor must be between 0 and 1.")
  }
}
