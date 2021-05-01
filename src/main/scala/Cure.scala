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
    val inputDir = "file://" + currentDir + "/datasets/data_size1/output2.txt"
    val outputFile = "file://" + currentDir + "/output"

    val startTime = System.currentTimeMillis()

    val cureArgs = CureArgs(0.95, 10, 10, 0.3, 4, inputDir, 0.1, removeOutliers = true)
    val result = CureAlgorithm.start(cureArgs, sc)

    val resultFile = outputFile + "_" + new Date().getTime.toString
    result.saveAsTextFile(resultFile)
    val endTime = System.currentTimeMillis()
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val output = fs.create(new Path(s"$resultFile/runtime.txt"))
    val writer = new PrintWriter(output)
    val text = s"Total time taken to assign clusters is : ${((endTime - startTime) * 1.0) / 1000} seconds"
    println(text)
    try {
      writer.write(text)
      writer.write(System.lineSeparator())
    }
    finally
      writer.close()

    print(System.lineSeparator() + "Job Completed Successfully")

    sc.stop()
  }
}
