package clustering.cure

import clustering.structures.{Cluster, KDNode, KDPoint, KDTree, MinHeap}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object CureAlgorithm {

  def start(cureArgs: CureArgs, sparkContext: SparkContext): RDD[String] = {

    val file = cureArgs.inputFile

    println(s"SPARK CONFIGS are ${
      sparkContext
        .getConf
        .getAll
        .mkString("", ", ", "")
    }")
    val broadcastK = sparkContext.broadcast(cureArgs.clusters)
    val broadcastM = sparkContext.broadcast(cureArgs.representatives)
    val broadcastRemoveOutliers = sparkContext.broadcast(cureArgs.removeOutliers)
    val sf = cureArgs.shrinkingFactor
    val broadcastSf = sparkContext.broadcast(sf)
    val distFile = sparkContext.textFile(file)
    val size = distFile.count()
    val m = cureArgs.representatives
    println(s"Size of input file $file is $size")

    val dimensions = distFile
      .top(1)
      .head
      .split(",")
      .length
    println(s"Sensed the records in data have $dimensions dimensions")
    val broadcastDimen = sparkContext.broadcast(dimensions)
    val sample = distFile
      .sample(withReplacement = false, fraction = cureArgs.samplingRatio)
      .repartition(cureArgs.partitions)
    println(s"The total size is $size and sampled count is ${sample.count()}")

    val points: RDD[KDPoint] = sample.map(a => {
      val p = KDPoint(a.split(",").slice(0, broadcastDimen.value).map(_.toDouble))
      p.cluster = Cluster(Array(p), Array(p), null, p)
      p
    }).cache()

    val clusters = points.mapPartitions(partition => {
      val numRepresentatives = broadcastM.value
      val data = partition.toList
      val removeOutliers = broadcastRemoveOutliers.value
      val shrinkf = broadcastSf.value
      val numClusters = if (removeOutliers) broadcastK.value * 2 else broadcastK.value

      if (data.lengthCompare(numClusters) > 0) {
        val kdTree: KDTree = createKDTree(data)
        println("Created Kd Tree in partition")
        val cHeap: MinHeap = createHeap(data, kdTree)

        computeClustersAtPartitions(numClusters, numRepresentatives, shrinkf, kdTree, cHeap)
        if (removeOutliers) {
          0 until cHeap.heapSize foreach (i => if (cHeap.getDataArray(i).representatives.length < m) cHeap.remove(i))
          computeClustersAtPartitions(broadcastK.value, numRepresentatives, shrinkf, kdTree, cHeap)
        }
        cHeap.getDataArray.slice(0, cHeap.heapSize).map(cc => {
          cc.points.foreach(_.cluster = null)
          val reps = cc.representatives
          Cluster(findMFarthestPoints(cc.points, cc.mean, numRepresentatives), reps, null, cc.mean, cc.squaredDistance)
        }).toIterator
      }
      else {
        data.map(a => {
          Cluster(Array(a), Array(a), null, a)
        }).toIterator
      }
    }
    )

    val cureClusters = clusters.collect()
    println(s"Partitioned Execution finished Successfully. Collected all ${cureClusters.length} clusters at driver")

    cureClusters.foreach(c => c.representatives.foreach(a => {
      if (a != null) a.cluster = c
    }))

    val reducedPoints = cureClusters.flatMap(_.representatives).toList
    val kdTree: KDTree = createKDTree(reducedPoints)
    val cHeap: MinHeap = createHeapFromClusters(cureClusters.toList, kdTree)

    var clustersShortOfMReps = if (cureArgs.removeOutliers) cureClusters.count(_.representatives.length < m) else 0
    while (cHeap.heapSize - clustersShortOfMReps > cureArgs.clusters) {
      val c1 = cHeap.takeHead()
      val nearest = c1.nearest
      val c2 = mergeClusterWithPointsAndRep(c1, nearest, cureArgs.representatives, sf)

      if (cureArgs.removeOutliers) {
        val a = nearest.representatives.length < m
        val b = c1.representatives.length < m
        val c = c2.representatives.length < m

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
    println(s"Merged clusters at driver. Total clusters ${cHeap.heapSize} Removed $clustersShortOfMReps clusters without $m representatives")
    val finalClusters = cHeap.getDataArray.slice(0, cHeap.heapSize).filter(_.representatives.length >= m)
    finalClusters.zipWithIndex.foreach { case (x, i) => x.id = i }
    println("Final Representatives")
    finalClusters.foreach(c => c.representatives.foreach(r => println(s"$r , ${c.id}")))
    val kdBroadcast = sparkContext.broadcast(kdTree)
    println("Broadcasting kdTree from driver to executors")

    distFile.mapPartitions(partn => {
      val kdTreeAtEx = kdBroadcast.value
      partn.map(p => {
        val readPoint = KDPoint(p.split(',').slice(0, broadcastDimen.value).map(_.toDouble))
        p.concat(s",${kdTreeAtEx.closestPointOfOtherCluster(readPoint).cluster.id}")
      })
    })
  }

  private def computeClustersAtPartitions(numClusters: Int, numRepresentatives: Int, shrinkf: Double, kdTree: KDTree, cHeap: MinHeap): Unit = {
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

  private def createKDTree(data: List[KDPoint]): KDTree = {
    val kdTree = KDTree(KDNode(data.head, null, null), data.head.dimensions.length)
    for (i <- 1 until data.length)
      kdTree.insert(data(i))
    kdTree
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
      if (mergedPoints.length <= repCount) {
        Cluster(mergedPoints, shrinkRepresentativeArray(sf, mergedPoints, mean), null, mean)
      }
      else {
        val tmpArray = new Array[KDPoint](repCount)
        for (i <- 0 until repCount) {
          var maxDist = 0.0d
          var minDist = 0.0d
          var maxPoint: KDPoint = null
          mergedPoints.foreach(p => {
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

        val representatives = shrinkRepresentativeArray(sf, tmpArray, mean)
        val newCluster = Cluster(mergedPoints, representatives, null, mean)
        newCluster
      }
    }

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


  private def shrinkRepresentativeArray(sf: Double, tmpArray: Array[KDPoint], mean: KDPoint): Array[KDPoint] = {
    val repArray = copyPointsArray(tmpArray)
    repArray.foreach(rep => {
      if (rep == null)
        return null
      val repDim = rep.dimensions
      repDim.indices
        .foreach(i => repDim(i) += (mean.dimensions(i) - repDim(i)) * sf)
    })
    repArray
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
