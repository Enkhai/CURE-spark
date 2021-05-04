package clustering.cure

import clustering.structures.{Cluster, KDNode, KDPoint, KDTree, MinHeap}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object CureAlgorithm {

  def start(cureArgs: CureArgs, sparkContext: SparkContext): RDD[(Array[Double], Int)] = {

    // read data
    val distFile = sparkContext.textFile(cureArgs.inputFile)
      .map(_
        .split(",")
        .map(_.toDouble)
      )

    // sample the data
    val sample = distFile
      .sample(withReplacement = false, fraction = cureArgs.samplingRatio)
      .repartition(cureArgs.partitions)
    println(s"The total size is ${distFile.count()} and sampled count is ${sample.count()}")

    // create a new cluster for each point
    val points = sample.map(a => {
      val p = KDPoint(a)
      p.cluster = Cluster(Array(p), Array(p), null, p)
      p
    }).cache()

    // broadcast needed variables for creating the clusters
    val broadcastVariables = (sparkContext.broadcast(cureArgs.clusters),
      sparkContext.broadcast(cureArgs.representatives),
      sparkContext.broadcast(cureArgs.shrinkingFactor),
      sparkContext.broadcast(cureArgs.removeOutliers))

    // make the clusters
    val clusters = points.mapPartitions(partition => cluster(partition, broadcastVariables))
      .collect()

    println(s"Partitioned Execution finished Successfully. Collected all ${clusters.length} clusters at driver")

    // all representatives should reference their cluster
    clusters.foreach(c => c.representatives.foreach(a => {
      if (a != null) a.cluster = c
    }))

    // gather the representative points and build a tree and a heap anew
    val reducedPoints = clusters.flatMap(_.representatives).toList
    val kdTree: KDTree = createKDTree(reducedPoints)
    val cHeap: MinHeap = createHeapFromClusters(clusters.toList, kdTree)

    // count the number of clusters not satisfying the desired number of representatives
    var clustersShortOfMReps =
      if (cureArgs.removeOutliers)
        clusters.count(_.representatives.length < cureArgs.representatives)
      else
        0

    // trim all clusters having less than the desired number of representatives
    while (cHeap.heapSize - clustersShortOfMReps > cureArgs.clusters) {
      val c1 = cHeap.takeHead()
      val nearest = c1.nearest
      val c2 = mergeClusterWithPointsAndRep(c1, nearest, cureArgs.representatives, cureArgs.shrinkingFactor)

      if (cureArgs.removeOutliers) {
        val a = nearest.representatives.length < cureArgs.representatives
        val b = c1.representatives.length < cureArgs.representatives
        val c = c2.representatives.length < cureArgs.representatives

        if (a && b && c) clustersShortOfMReps = clustersShortOfMReps - 1
        else if (a && b) clustersShortOfMReps = clustersShortOfMReps - 2
        else if (a || b) clustersShortOfMReps = clustersShortOfMReps - 1
      }

      c1.representatives.foreach(kdTree.delete)
      nearest.representatives.foreach(kdTree.delete)

      val representArray = c2.representatives
      val (newNearestCluster, nearestDistance) = getNearestCluster(representArray, kdTree)
      c2.nearest = newNearestCluster
      c2.squaredDistance = nearestDistance

      representArray.foreach(kdTree.insert)
      removeClustersFromHeapUsingReps(kdTree, cHeap, c1, nearest)
      cHeap.insert(c2)
      println(s"Processing and merging clusters. Heap size is :: ${cHeap.heapSize}")
    }
    println(s"Merged clusters at driver.\n" +
      s"  Total clusters ${cHeap.heapSize}\n" +
      s"  Removed $clustersShortOfMReps clusters without $cureArgs.representatives representatives")


    val finalClusters = cHeap.getDataArray
      .slice(0, cHeap.heapSize)
      .filter(_.representatives.length >= cureArgs.representatives)
    finalClusters.zipWithIndex
      .foreach { case (x, i) => x.id = i }
    println("Final Representatives")
    finalClusters
      .foreach(c =>
        c.representatives
          .foreach(r => println(s"$r , ${c.id}"))
      )
    val broadcastTree = sparkContext.broadcast(kdTree)
    println("Broadcasting kdTree from driver to executors")

    distFile.mapPartitions(partn => {
      val kdTreeAtEx = broadcastTree.value
      partn.map(p => {
        val readPoint = KDPoint(p)
        (p, kdTreeAtEx.closestPointOfOtherCluster(readPoint).cluster.id)
      })
    })
  }

  private def cluster(partition: Iterator[KDPoint],
                      broadcasts: (Broadcast[Int],
                        Broadcast[Int],
                        Broadcast[Double],
                        Broadcast[Boolean])): Iterator[Cluster] = {

    val (clusters, representatives, shrinkingFactor, removeOutliers) = broadcasts

    val partitionList = partition.toList

    // if the provided points are already less than or equal to the desired number of clusters
    // we can simply return them as clusters
    if (partitionList.length <= clusters.value)
      return partitionList
        .map(a => Cluster(Array(a), Array(a), null, a))
        .toIterator

    val kdTree: KDTree = createKDTree(partitionList)
    val cHeap: MinHeap = createHeap(partitionList, kdTree)

    // if outlier filtering is enabled we set the number of clusters to the double of the default (order of 2)
    // and filter the clusters when we have reached that number
    if (removeOutliers.value) {
      computeClustersAtPartitions(clusters.value * 2, representatives.value, shrinkingFactor.value, kdTree, cHeap)
      for (i <- 0 until cHeap.heapSize)
        if (cHeap.getDataArray(i).representatives.length < representatives.value)
          cHeap.remove(i)
    }
    // clustering computation will continue with the default number of clusters
    computeClustersAtPartitions(clusters.value, representatives.value, shrinkingFactor.value, kdTree, cHeap)

    cHeap.getDataArray
      .slice(0, cHeap.heapSize)
      .map(cc => {
        cc.points.foreach(_.cluster = null)
        val reps = cc.representatives
        Cluster(findMFarthestPoints(cc.points, cc.mean, representatives.value), reps, null, cc.mean, cc.squaredDistance)
      }).toIterator
  }

  // OK
  private def createKDTree(data: List[KDPoint]): KDTree = {
    val kdTree = KDTree(KDNode(data.head, null, null), data.head.dimensions.length)
    for (i <- 1 until data.length)
      kdTree.insert(data(i))
    kdTree
  }

  // OK
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
      val (closest, distance) = getNearestCluster(p.representatives, kdTree)
      p.nearest = closest
      p.squaredDistance = distance
      cHeap.insert(p)
    })
    cHeap
  }

  private def computeClustersAtPartitions(numClusters: Int,
                                          numRepresentatives: Int,
                                          shrinkf: Double,
                                          kdTree: KDTree,
                                          cHeap: MinHeap): Unit = {
    var i = 0
    while (cHeap.heapSize > numClusters) {
      val c1 = cHeap.takeHead()
      val nearest = c1.nearest
      val c2 = mergeClusterWithPointsAndRep(c1, nearest, numRepresentatives, shrinkf)
      c1.representatives.foreach(kdTree.delete)
      nearest.representatives.foreach(kdTree.delete)
      val (newNearestCluster, nearestDistance) = getNearestCluster(c2.representatives, kdTree)
      c2.nearest = newNearestCluster
      c2.squaredDistance = nearestDistance
      c2.representatives.foreach(kdTree.insert)
      removeClustersFromHeapUsingReps(kdTree, cHeap, c1, nearest)
      if (i % 256 == 0) println(s"Processing and merging clusters from heap. Current Total Cluster size is ${cHeap.heapSize}")
      i = i + 1
      cHeap.insert(c2)
    }
  }

  private def removeClustersFromHeapUsingReps(kdTree: KDTree, cHeap: MinHeap, c1: Cluster, nearest: Cluster): Unit = {
    val heapArray = cHeap.getDataArray
    val heapSize = cHeap.heapSize
    var it = 0
    while (it < heapSize) {
      var flag = false
      val tmpCluster = heapArray(it)
      val tmpNearest = tmpCluster.nearest
      if (tmpCluster == nearest) {
        cHeap.remove(it) //remove cluster
        flag = true
      }
      if (tmpNearest == nearest || tmpNearest == c1) {
        val (newCluster, newDistance) = getNearestCluster(tmpCluster.representatives, kdTree)
        tmpCluster.nearest = newCluster
        tmpCluster.squaredDistance = newDistance
        cHeap.heapify(it)
        flag = true
      }
      if (!flag) it = it + 1
    }
  }


  private def getNearestCluster(points: Array[KDPoint], kdTree: KDTree): (Cluster, Double) = {
    val (point, distance) = points.foldLeft(points(0), Double.MaxValue) {
      case ((nearestPoint, newD), rep) =>
        val closest = kdTree.closestPointOfOtherCluster(rep)
        val d = rep.squaredDistance(closest)
        if (d < newD) (closest, d)
        else (nearestPoint, newD)
    }
    (point.cluster, distance)
  }

  // OK
  def copyPointsArray(oldArray: Array[KDPoint]): Array[KDPoint] = {
    oldArray
      .clone()
      .map(x => {
        if (x == null)
          return null
        KDPoint(x.dimensions.clone())
      })
  }

  def mergeClusterAndPoints(c1: Cluster, nearest: Cluster): Cluster = {
    val mergedPoints = c1.points ++ nearest.points
    val mean = meanOfPoints(mergedPoints)
    val newCluster = Cluster(mergedPoints, null, null, mean)
    mergedPoints.foreach(_.cluster = newCluster)
    mean.cluster = newCluster
    newCluster
  }

  private def mergeClusterWithPointsAndRep(c1: Cluster, nearest: Cluster, repCount: Int, sf: Double): Cluster = {

    val mergedPoints = c1.points ++ nearest.points
    val mean = meanOfPoints(mergedPoints)

    val mergedCl = {
      if (mergedPoints.length <= repCount)
        Cluster(mergedPoints, shrinkRepresentativeArray(sf, mergedPoints, mean), null, mean)
      else {
        val tmpArray = findMFarthestPoints(mergedPoints, mean, repCount)
        Cluster(mergedPoints, shrinkRepresentativeArray(sf, tmpArray, mean), null, mean)
      }
    }

    mergedCl.representatives.foreach(_.cluster = mergedCl)
    mergedCl.points.foreach(_.cluster = mergedCl)
    mergedCl.mean.cluster = mergedCl
    mergedCl
  }

  // OK
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


  // OK
  private def shrinkRepresentativeArray(sf: Double, repArray: Array[KDPoint], mean: KDPoint): Array[KDPoint] = {
    val tmpArray = copyPointsArray(repArray)
    tmpArray.foreach(rep => {
      if (rep == null)
        return null
      val repDim = rep.dimensions
      repDim.indices
        .foreach(i => repDim(i) += (mean.dimensions(i) - repDim(i)) * sf)
    })
    tmpArray
  }

  // OK
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
