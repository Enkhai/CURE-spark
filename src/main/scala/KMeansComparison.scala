import clustering.hierarchical.AgglomerativeAlgorithm
import clustering.structures.{Cluster, KDPoint}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j._
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.{SparkConf, SparkContext}

import java.io.PrintWriter
import java.util.Date

object KMeansComparison {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark.SparkContext").setLevel(Level.WARN)

    val sparkConf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("KMeansComparison")

    val sc = new SparkContext(sparkConf)

    val currentDir = System.getProperty("user.dir")
    val inputDir = "file://" + currentDir + "/datasets/data_size2/data1.txt"
    val outputDir = "file://" + currentDir + "/kmeansAgglomerativeOutput"

    val parsedData = sc.textFile(inputDir)
      .map(s => Vectors.dense(s.split(',').map(_.toDouble)))
      .cache()

    val numClusters = 40
    val maxIterations = 100

    var startTime = System.currentTimeMillis()
    val model = KMeans.train(parsedData, numClusters, maxIterations)
    val predictions = model.predict(parsedData)
    var endTime = System.currentTimeMillis()

    var text = s"Total time taken for computing KMeans predictions: ${(endTime - startTime) * 1.0 / 1000}\n"

    val WSSSE = model.computeCost(parsedData)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

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
        val cluster = Cluster(points, null, null, null, id = x._1)
        points.foreach(_.cluster = cluster)
        cluster
      }
      ).collect()

    startTime = System.currentTimeMillis()
    val result = AgglomerativeAlgorithm.start(clusters, 5)
    endTime = System.currentTimeMillis()

    text += s"Total time taken for computing Agglomerative clustering predictions: " +
      s"${(endTime - startTime) * 1.0 / 1000}\n"
    println(text)

    val resultFolder = outputDir + "_" + new Date().getTime.toString
    val stringResults = result.map(x =>
      x._1
        .mkString(",")
        .concat(s",${x._2}")
    )

    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    fs.mkdirs(new Path(resultFolder))
    val resultsOutput = fs.create(new Path(s"$resultFolder/results.txt"))
    val runtimeOutput = fs.create(new Path(s"$resultFolder/runtime.txt"))

    val resultsTimeWriter = new PrintWriter(resultsOutput)
    try {
      for (r <- stringResults)
        resultsTimeWriter.write(r)
    } finally
      resultsTimeWriter.close()

    val runtTimeWriter = new PrintWriter(runtimeOutput)
    try
      runtTimeWriter.println(text)
    finally
      runtTimeWriter.close()

    sc.stop()
  }
}