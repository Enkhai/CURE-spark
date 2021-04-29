import org.apache.log4j._
import org.apache.spark.{SparkConf, SparkContext}
import clustering.cure.CureClustering__

object CUREBase {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark.SparkContext").setLevel(Level.WARN)

    val sparkConf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("CUREBase")

    val sc = new SparkContext(sparkConf)

    val currentDir = System.getProperty("user.dir")
    val inputDir = "file://" + currentDir + "/datasets/data_size1"
    val outputDir = "file://" + currentDir + "/output"

    val numClusters = 4
    var points = sc.textFile(inputDir + "/*", numClusters)
      .map(_.split(","))
      .map(_.map(_.toDouble))

    val timeBefore = System.nanoTime()

    val cureClf = new CureClustering__()
    val result = cureClf.fitPredict(points)

    result.map(x => (x._1.mkString(",") + x._2.toString)).saveAsTextFile(outputDir)

    print("######### Time taken for CURE clustering #########\n")
    print((System.nanoTime - timeBefore) / 1e9d)

    sc.stop()
  }
}
