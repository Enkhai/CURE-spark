import org.apache.log4j._
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

object KMeansComparison {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark.SparkContext").setLevel(Level.WARN)

    val sparkConf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("KMeansComparison")

    val sc = new SparkContext(sparkConf)

    val currentDir = System.getProperty("user.dir")
    val inputDir = "file://" + currentDir + "/datasets/data_size1"
    val outputDir = "file://" + currentDir + "/output"

    val parsedData = sc.textFile(inputDir).map(s => Vectors.dense(s.split(',').map(_.toDouble))).cache()

    // Cluster the data into two classes using KMeans
    val numClusters = 2
    val numIterations = 20
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    val predictions = clusters.predict(parsedData)
    predictions.collect()


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