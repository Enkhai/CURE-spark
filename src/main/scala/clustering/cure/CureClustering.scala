package clustering.cure

import org.apache.spark.rdd.RDD
import clustering.agglomerative.{AgglomerativeClustering, Cluster}
import clustering.Misc.euclideanDistance

import scala.collection.mutable.ArrayBuffer

class CureClustering(numClusters: Int = 5, shrinkingFactor: Double = 0.2, sampleSize: Int = 10000,
                     numRepresentatives: Int = 10) {

  validateArgs()

  def fitPredict(rdd: RDD[Array[Double]]): RDD[(Array[Double], Int)] = {
    val sample = rdd.takeSample(withReplacement = false, sampleSize)

    val clusters = new AgglomerativeClustering(numClusters).fitPredict(sample)
    var representativeClusters = findRepresentatives(clusters)

    representativeClusters = representativeClusters
      .map(x => (x._1.map(
        y => y.zip(clusters(x._2)._1.centroid)
          .map { case (i, j) => i + (j - i) * shrinkingFactor }
      ), x._2))

    val representatives = representativeClusters.flatMap({ case (x, i) => x.map(y => (y, i)) })

    rdd.mapPartitions(partition => classifyPartitionPoints(partition, representatives))
  }

  private def classifyPartitionPoints(partition: Iterator[Array[Double]],
                                      representatives: Array[(Array[Double], Int)]): Iterator[(Array[Double], Int)] = {
    partition.map(x => (x,
      representatives
        .map(r => (euclideanDistance(r._1, x), r._2))
        .minBy(_._1)
        ._2
    )
    )
  }

  private def findRepresentatives(clusters: Array[(Cluster, Int)]): Array[(Array[Array[Double]], Int)] = {
    val representatives = new Array[ArrayBuffer[Array[Double]]](numClusters)
    for (i <- clusters.indices) {
      val cluster_points = clusters(i)._1.getPoints

      var currRepresentative = cluster_points
        .map(x => (x, euclideanDistance(x, clusters(i)._1.centroid)))
        .zipWithIndex
        .maxBy(_._1._2)
      cluster_points.drop(currRepresentative._2)

      representatives(i).append(currRepresentative._1._1)

      for (_ <- 1 until numRepresentatives) {
        currRepresentative = representatives(i).map(x => (x,
          euclideanDistance(representatives(i)
            .transpose
            .map(x => {
              x.sum / x.length
            }).toArray, x)))
          .zipWithIndex
          .maxBy(_._1._2)

        cluster_points.drop(currRepresentative._2)
        representatives(i).append(currRepresentative._1._1)
      }
    }
    representatives.map(_.toArray).zipWithIndex
  }

  private def validateArgs() {
    if (numClusters < 1)
      throw new Exception("Please specify more than two clusters")
    if (0 > shrinkingFactor || shrinkingFactor > 0.5)
      throw new Exception("Attribute shrinking factor must be higher than 0 but lower than 0.5")
    if (numRepresentatives <= 1)
      throw new Exception("Please specify one or more representatives for a cluster")
  }
}
