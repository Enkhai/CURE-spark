package clustering.cure

import clustering.structures.{Cluster, KDNode, KDPoint, KDTree, MinHeap}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

object CureAlgorithm {

  def start(cureArgs: CureArgs, sparkContext: SparkContext):
  (RDD[(Array[Double], Int)], Array[(Array[Double], Int)], Array[(Array[Double], Int)]) = {

    val distFile = sparkContext.textFile(cureArgs.inputFile)
      .map(_
        .split(",")
        .map(_.toDouble)
      )

    val sample = distFile
      .sample(withReplacement = false, fraction = cureArgs.samplingRatio)
      .repartition(cureArgs.partitions)
    println(s"The total size is ${distFile.count()} and sampled count is ${sample.count()}")

    val points = sample.map(a => {
      val p = KDPoint(a)
      p.cluster = Cluster(Array(p), Array(p), null, p)
      p
    }).cache()

    val broadcastVariables = (sparkContext.broadcast(cureArgs.numClusters),
      sparkContext.broadcast(cureArgs.numRepresentatives),
      sparkContext.broadcast(cureArgs.shrinkingFactor),
      sparkContext.broadcast(cureArgs.removeOutliers))

    val clusters = points.mapPartitions(partition => cluster(partition, broadcastVariables))
      .collect()
    println(s"Partitioned Execution finished Successfully. Collected all ${clusters.length} clusters at driver.")

    val reducedPoints = clusters.flatMap(_.representatives).toList
    val kdTree = createKDTree(reducedPoints)
    val cHeap = createHeapFromClusters(clusters.toList, kdTree)

    var clustersShortOfMReps =
      if (cureArgs.removeOutliers)
        clusters.count(_.representatives.length < cureArgs.numRepresentatives)
      else
        0

    // trim all clusters having less than the desired number of representatives
    while (cHeap.heapSize - clustersShortOfMReps > cureArgs.numClusters) {
      val c1 = cHeap.takeHead()
      val nearest = c1.nearest
      val c2 = merge(c1, nearest, cureArgs.numRepresentatives, cureArgs.shrinkingFactor)

      if (cureArgs.removeOutliers) {
        val a = nearest.representatives.length < cureArgs.numRepresentatives
        val b = c1.representatives.length < cureArgs.numRepresentatives
        val c = c2.representatives.length < cureArgs.numRepresentatives

        if (a && b && c) clustersShortOfMReps = clustersShortOfMReps - 1
        else if (a && b) clustersShortOfMReps = clustersShortOfMReps - 2
        else if (a || b) clustersShortOfMReps = clustersShortOfMReps - 1
      }

      c1.representatives.foreach(kdTree.delete)
      nearest.representatives.foreach(kdTree.delete)

      val (newNearestCluster, nearestDistance) = getNearestCluster(c2, kdTree)
      c2.nearest = newNearestCluster
      c2.squaredDistance = nearestDistance

      c2.representatives.foreach(kdTree.insert)
      removeClustersFromHeapUsingReps(kdTree, cHeap, c1, nearest)
      cHeap.insert(c2)
      println(s"Processing and merging clusters. Heap size is ${cHeap.heapSize}")
    }
    println(s"Merged clusters at driver.\n" +
      s"  Total clusters ${cHeap.heapSize}\n" +
      s"  Removed $clustersShortOfMReps clusters without ${cureArgs.numRepresentatives} representatives")

    val finalClusters = cHeap.getDataArray
      .slice(0, cHeap.heapSize)
      .filter(_.representatives.length >= cureArgs.numRepresentatives)
    finalClusters.zipWithIndex
      .foreach { case (x, i) => x.id = i }

    val broadcastTree = sparkContext.broadcast(kdTree)
    val result = distFile.mapPartitions(partition => {
      partition.map(p => {
        (p, broadcastTree.value
          .closestPointOfOtherCluster(KDPoint(p))
          .cluster
          .id)
      })
    })

    val finalRepresentatives = new ArrayBuffer[(Array[Double], Int)]
    val finalMeans = new ArrayBuffer[(Array[Double], Int)]
    finalClusters.foreach(c => {
      c.representatives.foreach(r => {
        finalRepresentatives += ((r.dimensions, c.id))
      })
      finalMeans += ((c.mean.dimensions, c.id))
    })

    println("Final Representatives")
    finalRepresentatives.foreach(r => println(s"(${r._1.mkString(",")}), ${r._2}"))

    (result, finalRepresentatives.toArray, finalMeans.toArray)
  }

  private def cluster(partition: Iterator[KDPoint],
                      broadcasts: (Broadcast[Int],
                        Broadcast[Int],
                        Broadcast[Double],
                        Broadcast[Boolean])): Iterator[Cluster] = {

    val (numClusters, numRepresentatives, shrinkingFactor, removeOutliers) = broadcasts

    val partitionList = partition.toList

    if (partitionList.length <= numClusters.value)
      return partitionList
        .map(p => Cluster(Array(p), Array(p), null, p))
        .toIterator

    val kdTree = createKDTree(partitionList)
    val cHeap = createHeap(partitionList, kdTree)

    if (removeOutliers.value) {
      computeClustersAtPartitions(numClusters.value * 2, numRepresentatives.value, shrinkingFactor.value, kdTree, cHeap)
      for (i <- 0 until cHeap.heapSize)
        if (cHeap.getDataArray(i).representatives.length < numRepresentatives.value)
          cHeap.remove(i)
    }
    computeClustersAtPartitions(numClusters.value, numRepresentatives.value, shrinkingFactor.value, kdTree, cHeap)

    cHeap.getDataArray
      .slice(0, cHeap.heapSize)
      .map(c => {
        c.points.foreach(_.cluster = null)
        val newCluster = Cluster(findMFarthestPoints(c.points, c.mean, numRepresentatives.value),
          c.representatives,
          null,
          c.mean,
          c.squaredDistance)
        newCluster.representatives.foreach(_.cluster = newCluster)
        newCluster
      }).toIterator
  }

  private def createKDTree(data: List[KDPoint]): KDTree = {
    val kdTree = KDTree(KDNode(data.head, null, null), data.head.dimensions.length)
    for (i <- 1 until data.length)
      kdTree.insert(data(i))
    kdTree
  }

  private def createHeap(data: List[KDPoint], kdTree: KDTree) = {
    val cHeap = MinHeap(data.length)
    data.map(p => {
      val closest = kdTree.closestPointOfOtherCluster(p)
      p.cluster.nearest = closest.cluster
      p.cluster.squaredDistance = p.squaredDistance(closest)
      cHeap.insert(p.cluster)
      p.cluster
    })
    cHeap
  }

  private def createHeapFromClusters(data: List[Cluster], kdTree: KDTree): MinHeap = {
    val cHeap = MinHeap(data.length)
    data.foreach(p => {
      val (closest, distance) = getNearestCluster(p, kdTree)
      p.nearest = closest
      p.squaredDistance = distance
      cHeap.insert(p)
    })
    cHeap
  }

  private def computeClustersAtPartitions(numClusters: Int,
                                          numRepresentatives: Int,
                                          sf: Double,
                                          kdTree: KDTree,
                                          cHeap: MinHeap): Unit = {
    while (cHeap.heapSize > numClusters) {
      val c1 = cHeap.takeHead()
      val nearest = c1.nearest
      val c2 = merge(c1, nearest, numRepresentatives, sf)

      c1.representatives.foreach(kdTree.delete)
      nearest.representatives.foreach(kdTree.delete)

      val (newNearestCluster, nearestDistance) = getNearestCluster(c2, kdTree)
      c2.nearest = newNearestCluster
      c2.squaredDistance = nearestDistance
      c2.representatives.foreach(kdTree.insert)

      removeClustersFromHeapUsingReps(kdTree, cHeap, c1, nearest)

      cHeap.insert(c2)
    }
  }

  private def removeClustersFromHeapUsingReps(kdTree: KDTree, cHeap: MinHeap, cluster: Cluster, nearest: Cluster): Unit = {
    val heapSize = cHeap.heapSize
    var i = 0
    while (i < heapSize) {
      var continue = true
      val currCluster = cHeap.getDataArray(i)
      val currNearest = currCluster.nearest
      if (currCluster == nearest) {
        cHeap.remove(i)
        continue = false
      }
      if (currNearest == nearest || currNearest == cluster) {
        val (newCluster, newDistance) = getNearestCluster(currCluster, kdTree)
        currCluster.nearest = newCluster
        currCluster.squaredDistance = newDistance
        cHeap.heapify(i)
        continue = false
      }
      if (continue) i += 1
    }
  }

  private def getNearestCluster(cluster: Cluster, kdTree: KDTree): (Cluster, Double) = {
    val (nearestRep, nearestDistance) = cluster
      .representatives
      .foldLeft(null: KDPoint, Double.MaxValue) {
        case ((currNearestRep, currNearestDistance), rep) =>
          val nearestRep = kdTree.closestPointOfOtherCluster(rep)
          val nearestDistance = rep.squaredDistance(nearestRep)
          if (nearestDistance < currNearestDistance)
            (nearestRep, nearestDistance)
          else
            (currNearestRep, currNearestDistance)
      }
    (nearestRep.cluster, nearestDistance)
  }

  def copyPointsArray(oldArray: Array[KDPoint]): Array[KDPoint] = {
    oldArray
      .clone()
      .map(p => {
        if (p == null)
          return null
        KDPoint(p.dimensions.clone())
      })
  }

  private def merge(cluster: Cluster, nearest: Cluster, repCount: Int, sf: Double): Cluster = {
    val mergedPoints = cluster.points ++ nearest.points
    val mean = meanOfPoints(mergedPoints)
    var representatives = mergedPoints
    if (mergedPoints.length > repCount)
      representatives = findMFarthestPoints(mergedPoints, mean, repCount)
    representatives = shrinkRepresentativeArray(sf, representatives, mean)

    val mergedCl = Cluster(mergedPoints, representatives, null, mean)

    mergedCl.representatives.foreach(_.cluster = mergedCl)
    mergedCl.points.foreach(_.cluster = mergedCl)
    mergedCl.mean.cluster = mergedCl

    mergedCl
  }

  private def findMFarthestPoints(points: Array[KDPoint], mean: KDPoint, m: Int): Array[KDPoint] = {
    val tmpArray = new Array[KDPoint](m)
    for (i <- 0 until m) {
      var maxDist = 0.0d
      var minDist = 0.0d
      var maxPoint: KDPoint = null

      points.foreach(p => {
        if (!tmpArray.contains(p)) {
          if (i == 0) minDist = p.squaredDistance(mean)
          else {
            minDist = tmpArray.foldLeft(Double.MaxValue) { (maxd, r) => {
              if (r == null) maxd
              else {
                val dist = p.squaredDistance(r)
                if (dist < maxd) dist
                else maxd
              }
            }
            }
          }
          if (minDist >= maxDist) {
            maxDist = minDist
            maxPoint = p
          }
        }
      })
      tmpArray(i) = maxPoint
    }
    tmpArray.filter(_ != null)
  }

  private def shrinkRepresentativeArray(sf: Double, repArray: Array[KDPoint], mean: KDPoint): Array[KDPoint] = {
    val tmpArray = copyPointsArray(repArray)
    tmpArray.foreach(rep => {
      val repDim = rep.dimensions
      repDim.indices
        .foreach(i => repDim(i) += (mean.dimensions(i) - repDim(i)) * sf)
    })
    tmpArray
  }

  def meanOfPoints(points: Array[KDPoint]): KDPoint = {
    KDPoint(points
      .filter(_ != null)
      .map(_.dimensions)
      .transpose
      .map(x => {
        x.sum / x.length
      }))
  }
}
