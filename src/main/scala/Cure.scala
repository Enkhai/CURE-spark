import clustering.cure.{CureAlgorithm, CureArgs}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j._
import org.apache.spark.{SparkConf, SparkContext}

import java.io.PrintWriter
import java.util.Date

object Cure {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark.SparkContext").setLevel(Level.WARN)

    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Cure")

    val sc = new SparkContext(sparkConf)

    val currentDir = System.getProperty("user.dir")
//    val inputDir = "file://" + currentDir + "/datasets/data2_10.txt"
//    val inputDir = "file://" + currentDir + "/datasets/data2_100.txt"
    val inputDir = "file://" + currentDir + "/datasets/data2_10_w_outliers.txt"
//    val inputDir = "file://" + currentDir + "/datasets/data_size2/data1.txt"
//    val inputDir = "file://" + currentDir + "/datasets/data2_30_w_outliers.txt"
    val outputDir = "file://" + currentDir + "/cureOutput"

    val cureArgs = CureArgs(10, 4, 0.5, 5, inputDir, 0.08, removeOutliers = true)

    val startTime = System.currentTimeMillis()
    val (result, representatives, means) = CureAlgorithm.start(cureArgs, sc)
    result.cache()
    val endTime = System.currentTimeMillis()

    val text = s"Total time taken to assign clusters is : ${((endTime - startTime) * 1.0) / 1000} seconds"
    println(text)

    // write points
    val resultFile = outputDir + "_" + new Date().getTime.toString
    result.map(x =>
      x._1
        .mkString(",")
        .concat(s",${x._2}")
    ).saveAsTextFile(resultFile)

    val conf = new Configuration()
    val fs = FileSystem.get(conf)

    // write runtime results
    val runtimeOutput = fs.create(new Path(s"$resultFile/runtime.txt"))
    val runtimeWriter = new PrintWriter(runtimeOutput)
    try
      runtimeWriter.write(text)
    finally
      runtimeWriter.close()

    // write representatives
    val representativeOutput = fs.create(new Path(s"$resultFile/representatives.txt"))
    val representativeWriter = new PrintWriter(representativeOutput)
    try {
      for (r <- representatives){
        representativeWriter.println(
          r._1.mkString(",")
            .concat(",")
            .concat(r._2.toString)
        )
      }
    } finally
      representativeWriter.close()

    // write representative means
    val meanOutput = fs.create(new Path(s"$resultFile/means.txt"))
    val meanWriter = new PrintWriter(meanOutput)
    try {
      for (m <- means){
        meanWriter.println(
          m._1.mkString(",")
            .concat(",")
            .concat(m._2.toString)
        )
      }
    } finally
      meanWriter.close()

    sc.stop()
  }
}
