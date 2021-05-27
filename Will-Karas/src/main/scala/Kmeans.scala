import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.SparkContext._

import org.apache.log4j._
import org.apache.spark.{SparkConf, SparkContext}
import java.io.PrintWriter
import java.util.Date


object kmeans {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark.SparkContext").setLevel(Level.WARN)

    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("KMEANS")

    val sc = new SparkContext(sparkConf)

val currentDir = System.getProperty("user.dir")
val data = "file:///" + currentDir + "\\data_size1"
val outputDir = "file:///" + currentDir + "\\output_kmeans"


val startTime = System.currentTimeMillis()
val parsedData = sc.textFile(data).map(s => Vectors.dense(s.split(',').map(_.toDouble))).cache()

// Cluster the data into two classes using KMeans
val numClusters = 5

val numIterations = 30
val initializationmode= "KMeans.K_MEANS_PARALLEL"
val clusters = KMeans.train(parsedData, numClusters, numIterations, initializationMode = "k-means||")
val k = numClusters 
// Evaluate clustering by computing Within Set Sum of Squared Errors


val WSSSE = clusters.computeCost(parsedData)
println(s"Within Set Sum of Squared Errors or Compute Cost = $WSSSE")
val endTime = System.currentTimeMillis()

val text = s"Total time taken to assign clusters is : ${((endTime - startTime) * 1.0) / 1000} seconds"
println(text)

//Returns the Euclidian distance between vectors v1,v2
	def vecDist( v1:Vector, v2:Vector ) : Double = {
	  return Math.sqrt(Vectors.sqdist(v1,v2))
	}

	val clusteredData = parsedData.map(v => (clusters.predict(v),v)).cache() //Key: index of the appropriate cluster, Value: normalized point
					
	val indexedCenters = sc.parallelize(clusters.clusterCenters.map(cntr => (clusters.predict(cntr),cntr))).cache() //Key: index, Value: cluster center

	//Find the mean Silhouette score and the outliers for every cluster
	for (c <- 0 to k-1)
	{
		//The points from current cluster (c)
		val currData = clusteredData.filter(r => r._1 == c)
			.map(r => r._2).cache() //They are all from the same cluster so we throw the cluster index
		val indexedCurrData = currData.zipWithIndex().cache() //We index them in order to give an id to every point
		val numPoints = currData.count() //Number of points in this cluster

		//Cartesian product in order to compute each point's distance from all other points in the same cluster
		val intraClusterProduct = indexedCurrData.cartesian(indexedCurrData)
			.map{case (x,y) => (x._2,vecDist(x._1,y._1))} //Key: the first point's id (from when it was indexed), Value: its distance from the second point. 
		//In the cartesian product we also have points paired with themselves, but they don't affect our computations because their distance is 0 (doesn't affect the sum) and we divide by numPoints-1 (so that it doesn't affect the average)
		//ai = object i's average distance from the other objects in its cluster
		val ai = intraClusterProduct.reduceByKey((a,b) => a + b ) //Add the distances for elements with the same id (same point)
			.map(x =>(x._1, x._2/(numPoints-1))) //Key: point id, Value: average distance from its cluster
    			
		//All centers except the current one in order to find the closest to every point
		val otherCenters = indexedCenters.filter(x => x._1!=c )
		val nearestCenters = indexedCurrData.cartesian(otherCenters) //Every point from this cluster with every other center
			.map(r => (r._1._2,(r._2._1,r._1._1,vecDist(r._1._1,r._2._2)))) //Key: point id, Value: tuple (center index, point vector) 
			.reduceByKey((a,b) => if (a._3 < b._3) a else b) //Min distance for every id 
			.map(r => (r._2._1,(r._1, r._2._2))) //Key: index to center with min distance, Value: tuple(point id, point vector)
		//bi = object i's minimum average distance from objects of another cluster
		val bi = nearestCenters.join(clusteredData) //Join each point of this cluster with every point in the cluster with the nearest center (A workaround for computing bi granted that the data follow normal distribution)
			.map(r => (r._2._1._1,(vecDist(r._2._1._2,r._2._2),1))) //Key: first point's id, Value: tuple(distance of the two points,1)
			.reduceByKey((a,b) =>( a._1 + b._1, a._2 + b._2)) //Add the distances for every id (point) and count them 
			.map(r => (r._1, r._2._1/r._2._2)) //Key: point id, Value: average distance from nearest cluster

		def maxD(d1 : Double, d2: Double): Double = {
			if (d1>d2) return d1 else return d2
		}
		
		val s = ai.join(bi) //Join on point id
			.map(r => ((r._2._2 - r._2._1) / maxD(r._2._2,r._2._1), 1) ) //Key: Silhouette score for every point, Value: 1
			.reduce((a,b) => (a._1 + b._1, a._2 + b._2)) //Sum of silhouette scores, number of points
		val meanS = s._1 / s._2 //Average silhouette score for this cluster		

    println("Cluster "+ (c+1)) 
		println("Mean Silhouette Score: "+ meanS)	
		println()	
  }

// Save and load model
clusters.save(sc, outputDir)
// Export to PMML to a String in PMML format
//println(s"PMML Model:\n ${clusters.toPMML}")

// Export the model to a local file in PMML format
//clusters.toPMML(outputDir+"//kmeans.xml")

// Export the model to a directory on a distributed file system in PMML format
//clusters.toPMML(sc, outputDir+"//kmeans")

// Export the model to the OutputStream in PMML format
clusters.toPMML(System.out)

sc.stop() 
}
}