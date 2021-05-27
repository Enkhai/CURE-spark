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
    
    val startTime = System.currentTimeMillis()
    val currentDir = System.getProperty("user.dir")
    val inputDir = "file:///" + currentDir + "\\data1.txt"
    val outputDir = "file:///" + currentDir + "\\output_cure"


    val numClusters = 5
    val numRepresentatives=5
    val shrinkingFactor= 0.3
    val partitions = 1
    val inputFile = inputDir
    val samplingRatio = 0.4
    val removeOutliers= false

    val cureArgs = CureArgs(numClusters, numRepresentatives, shrinkingFactor, partitions, inputFile, samplingRatio, removeOutliers)

    
    val result = CureAlgorithm.start(cureArgs, sc).cache()
    println(result)
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

    sc.stop()
  }
}
