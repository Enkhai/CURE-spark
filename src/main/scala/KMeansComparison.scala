import clustering.structures.{Cluster, KDPoint}
import org.apache.log4j._
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.{SparkConf, SparkContext}

object KMeansComparison {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark.SparkContext").setLevel(Level.WARN)

    val sparkConf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("KMeansComparison")

    val sc = new SparkContext(sparkConf)

    val currentDir = System.getProperty("user.dir")
    val inputDir = "file://" + currentDir + "/datasets/data_size2/data1.txt"
    val outputDir = "file://" + currentDir + "/output"

    val parsedData = sc.textFile(inputDir)
      .map(s => Vectors.dense(s.split(',').map(_.toDouble)))
      .cache()

    val numClusters = 40
    val maxIterations = 100
    val model = KMeans.train(parsedData, numClusters, maxIterations)

    val WSSSE = model.computeCost(parsedData)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    val predictions = model.predict(parsedData)

    val mappedPredictions = predictions
      .zipWithIndex()
      .map(x => (x._2.toInt, x._1))
    val mappedData = parsedData
      .zipWithIndex()
      .map(x => (x._2.toInt, x._1))

    val dataWithPrediction = mappedData.join(mappedPredictions)
      .map(_._2)
    val clusters = dataWithPrediction.groupBy(_._2)
      .map(x => {
        val points = x._2
          .toArray
          .map(y => KDPoint(y._1.toArray))
        val cluster = Cluster(
          points,
          null,
          null,
          null,
          id = x._1)
        points.foreach(_.cluster = cluster)
        cluster
      }
      ).collect()
      .toList



    sc.stop()
  }
}