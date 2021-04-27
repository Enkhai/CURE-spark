package clustering

object Misc {

  def euclideanDistance(x: Array[Double], y: Array[Double]): Double = {
    Math.sqrt((x zip y).map { case (i, j) => Math.pow(i - j, 2) }.sum)
  }
}
