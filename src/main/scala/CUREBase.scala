import org.apache.log4j._
import org.apache.spark.{SparkConf, SparkContext}

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

    //TODO: load data points

    val timeBefore = System.nanoTime()

    //TODO: partition data points

    //TODO: run algorithm

    // dummy RDD - to be replaced by actual result
    var result = sc.parallelize(Array[Array[Double]](Array[Double](0, 1)))

    result.map(_.mkString(",")).saveAsTextFile(outputDir)

    print("######### Time taken for CURE clustering #########\n")
    print((System.nanoTime - timeBefore) / 1e9d)

    sc.stop()
  }
}
