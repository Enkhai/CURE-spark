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
      .setMaster("local[4]")
      .setAppName("Cure")

    val sc = new SparkContext(sparkConf)

    val currentDir = System.getProperty("user.dir")
    val inputDir = "file://" + currentDir + "/datasets/data_size2/data1.txt"
    val outputDir = "file://" + currentDir + "/output"

    val cureArgs = CureArgs(10, 10, 0.3, 4, inputDir, 0.4, removeOutliers = true)

    val startTime = System.currentTimeMillis()
    val result = CureAlgorithm.start(cureArgs, sc).cache()
    val endTime = System.currentTimeMillis()

    val text = s"Total time taken to assign clusters is : ${((endTime - startTime) * 1.0) / 1000} seconds"
    println(text)

    val resultFile = outputDir + "_" + new Date().getTime.toString
    result.map(x =>
      x._1
        .mkString(",")
        .concat(s",${x._2}")
    ).saveAsTextFile(resultFile)

    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val output = fs.create(new Path(s"$resultFile/runtime.txt"))
    val writer = new PrintWriter(output)
    try
      writer.write(text)
    finally
      writer.close()

    println("Job Completed Successfully")
    sc.stop()
  }
}
