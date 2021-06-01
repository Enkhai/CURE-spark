package clustering.cure

case class CureArgs(numClusters: Int,
                    numRepresentatives: Int,
                    shrinkingFactor: Double,
                    partitions: Int,
                    inputFile: String,
                    samplingRatio: Double = 1.0d,
                    removeOutliers: Boolean = false) {

  validateArgs()

  private def validateArgs(): Unit = {
    if (numClusters < 1)
      throw new Exception("Number of clusters should be larger than or equal to 1.")
    if (numRepresentatives <= 0)
      throw new Exception("Please specify a positive integer value > 0 for the number of representatives in a clusters")
    if (shrinkingFactor < 0 || shrinkingFactor > 1)
      throw new Exception("Attribute shrinking factor must be between and not including 0 and 1.")
    if (partitions < 0 || partitions > 100)
      throw new Exception("Please specify a positive integer value between 1 to 100 for the number of partitions.")
    if (samplingRatio < 0 || samplingRatio > 1)
      throw new Exception("Sampling ratio should between and not including 0 and 1.")
    println(s"Attributes for the CURE Algorithm:\n" +
      s"  Number of clusters:${numClusters}\n" +
      s"  Number of representatives:${numRepresentatives}\n" +
      s"  Shrinking Factor:${shrinkingFactor}\n" +
      s"  Number of partitions:${partitions}\n" +
      s"  Sampling ratio:${samplingRatio}")
    println(s"Reading data for Cure Algo from path $inputFile")
  }
}