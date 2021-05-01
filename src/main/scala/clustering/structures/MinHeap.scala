package clustering.structures

case class MinHeap(maxSize: Int) {
  private val data = new Array[Cluster](maxSize)
  private var size  = -1

  def insert(cluster:Cluster):Unit = {
    size += 1
    data(size) = cluster
    percolateUp(size)
  }

  def takeHead(): Cluster = {

    val head = data(0)
    data(0) = data(size)
    data(size) = null
    size-=1
    percolateDown(0)
    head
  }

  def update(index: Int, cluster: Cluster): Unit = {
    data(index) = cluster
    heapify(index)
  }

  def remove(index:Int):Unit = {
    data(index) = data(size)
    size-=1
    heapify(index)
  }

  def heapify(index: Int): Unit = {

    val parentI = index /2
    val lChild = index*2
    val rChild = lChild +1

    if(parentI > 0 && (data(parentI).squaredDistance > data(index).squaredDistance)) percolateUp(index)
    else percolateDown(index)
  }

  def getDataArray : Array[Cluster] = data
  def heapSize : Int = this.size + 1

  def percolateUp(curr: Int): Unit = {
    val pi = curr/2
    if(data(pi).squaredDistance > data(curr).squaredDistance){
      val tmp =data(pi)
      data(pi)=  data(curr)
      data(curr) = tmp
      percolateUp(pi)
    }
  }

  def percolateDown(curr: Int) : Unit= {

    val lChild = curr*2
    val rChild = lChild +1

    var min = {
      if(lChild <= size && data(lChild).squaredDistance < data(curr).squaredDistance) lChild
      else curr
    }
    min = {
      if(rChild <= size && data(rChild).squaredDistance < data(min).squaredDistance) rChild
      else min
    }

    if(min != curr){
      val tmp = data(min)
      data(min) = data(curr)
      data(curr) = tmp
      percolateDown(min)
    }
  }
}
