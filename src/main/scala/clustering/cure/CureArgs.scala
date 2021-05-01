package clustering.cure

case class CureArgs(confidence: Double,
                    clusters: Int,
                    representatives: Int,
                    shrinkingFactor: Double,
                    partitions: Int,
                    inputFile: String,
                    samplingRatio: Double = 1.0d,
                    removeOutliers: Boolean = false)