package clustering.structures

case class MinHeap(maxSize: Int) {
  private val data = new Array[Cluster](maxSize)
  private var size = -1

  def parent(index: Int): Int =
    index / 2

  def leftChild(index: Int): Int =
    index * 2

  def rightChild(index: Int): Int =
    index * 2 + 1

  def swap(a: Int, b: Int): Unit = {
    val tmp = data(a)
    data(a) = data(b)
    data(b) = tmp
  }

  def insert(cluster: Cluster): Unit = {
    size += 1
    data(size) = cluster
    percolateUp(size)
  }

  def takeHead(): Cluster = {
    val head = data(0)
    data(0) = data(size)
    data(size) = null
    size -= 1
    percolateDown(0)
    head
  }

  def update(index: Int, cluster: Cluster): Unit = {
    data(index) = cluster
    heapify(index)
  }

  def remove(index: Int): Unit = {
    data(index) = data(size)
    size -= 1
    heapify(index)
  }

  def heapify(index: Int): Unit = {
    val parentI = parent(index)
    if (parentI > 0 && (data(parentI).squaredDistance > data(index).squaredDistance))
      percolateUp(index)
    else
      percolateDown(index)
  }

  def getDataArray: Array[Cluster] =
    data

  def heapSize: Int =
    this.size + 1

  def percolateUp(curr: Int): Unit = {
    val parentI = parent(curr)
    if (data(parentI).squaredDistance > data(curr).squaredDistance) {
      swap(parentI, curr)
      percolateUp(parentI)
    }
  }

  def percolateDown(curr: Int): Unit = {
    val lChild = leftChild(curr)
    val rChild = rightChild(curr)

    var min = curr
    if (lChild <= size &&
      data(lChild).squaredDistance < data(curr).squaredDistance)
      min = lChild
    if (rChild <= size &&
      data(rChild).squaredDistance < data(min).squaredDistance)
      min = rChild

    if (min != curr) {
      swap(min, curr)
      percolateDown(min)
    }
  }
}
