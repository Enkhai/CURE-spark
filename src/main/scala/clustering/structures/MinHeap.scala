package clustering.structures

class MinHeap {
  var heap: Array[Cluster] = _
  var maxSize: Int = _
  var size: Int = _

  val FRONT: Int = 1

  def this(n: Int) {
    this
    heap = new Array[Cluster](n + 1)
    maxSize = n
    size = 0
  }

  def parent(key: Int): Int =
    key / 2

  def left(key: Int): Int =
    2 * key

  def right(key: Int): Int =
    2 * key + 1

  def isLeaf(key: Int): Boolean = {
    if (key >= (size / 2) && key <= size)
      return true
    false
  }

  def swap(fpos: Int, spos: Int): Unit = {
    val tmp: Cluster = heap(fpos)
    heap(fpos) = heap(spos)
    heap(spos) = tmp
  }

  def minHeapify(key: Int): Unit = {
    if (isLeaf(key))
      return

    val leftChild = left(key)
    val rightChild = right(key)

    var smallestChild = leftChild
    if (rightChild <= size && heap(rightChild).closestDistance < heap(leftChild).closestDistance)
      smallestChild = rightChild
    if (heap(key).closestDistance > heap(smallestChild).closestDistance) {
      swap(key, smallestChild)
      minHeapify(smallestChild)
    }
  }

  def insert(cluster: Cluster): Boolean = {

    if (size >= maxSize)
      return false

    size += 1
    heap(size) = cluster
    var current = size

    while (current > 1 && heap(current).closestDistance < heap(parent(current)).closestDistance) {
      swap(current, parent(current))
      current = parent(current)
    }
    true
  }

  def delete(cluster: Cluster): Unit = {
    var key = heap.indexOf(cluster)

    heap(key).closestDistance = Int.MinValue
    while (key > 1 && heap(key).closestDistance < heap(parent(key)).closestDistance) {
      swap(key, parent(key))
      key = parent(key)
    }
    extractMin()
  }

  def relocate(cluster: Cluster): Unit = {
    var key = heap.indexOf(cluster)
    if (heap(key).closestDistance == cluster.closestDistance)
      return
    if (heap(key).closestDistance < cluster.closestDistance) {
      heap(key) = cluster
      minHeapify(key)
    } else {
      heap(key) = cluster
      while (key > 1 && heap(key).closestDistance < heap(parent(key)).closestDistance) {
        swap(key, parent(key))
        key = parent(key)
      }
      extractMin()
    }
  }

  def minHeap(): Unit = {
    for (i <- (size / 2) to 1)
      minHeapify(i)
  }

  def extractMin(): Cluster = {
    val popped = heap(FRONT)
    heap(FRONT) = heap(size)
    size -= 1
    minHeapify(FRONT)
    popped
  }

  def getMin: Cluster = heap(FRONT)
}
