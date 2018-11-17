class CategorySortKey extends Ordered[CategorySortKey] with Serializable {
  private var clickCount = 0L
  private var orderCount = 0L
  private var payCount = 0L

  def this(clickCount: Long, orderCount: Long, payCount: Long) {
    this()
    this.clickCount = clickCount
    this.orderCount = orderCount
    this.payCount = payCount
  }

  override def $less(that: CategorySortKey): Boolean = {
    if (clickCount < that.getClickCount) return true
    else if (clickCount == that.getClickCount && orderCount < that.getOrderCount) return true
    else if (clickCount == that.getClickCount && orderCount == that.getOrderCount && payCount < that.getPayCount) return true
    false
  }

  override def $greater(that: CategorySortKey): Boolean = {
    if (clickCount > that.getClickCount) return true
    else if (clickCount == that.getClickCount && orderCount > that.getOrderCount) return true
    else if (clickCount == that.getClickCount && orderCount == that.getOrderCount && payCount > that.getPayCount) return true
    false
  }

  override def $less$eq(that: CategorySortKey): Boolean = {
    if ($less(that)) return true
    else if (clickCount == that.getClickCount && orderCount == that.getOrderCount && payCount == that.getPayCount) return true
    false
  }

  override def $greater$eq(that: CategorySortKey): Boolean = {
    if ($greater(that)) return true
    else if (clickCount == that.getClickCount && orderCount == that.getOrderCount && payCount == that.getPayCount) return true
    false
  }

  override def compare(that: CategorySortKey): Int = {
    if (clickCount - that.getClickCount != 0) return (clickCount - that.getClickCount).toInt
    else if (orderCount - that.getOrderCount != 0) return (orderCount - that.getOrderCount).toInt
    else if (payCount - that.getPayCount != 0) return (payCount - that.getPayCount).toInt
    0
  }

  override def compareTo(that: CategorySortKey): Int = {
    if (clickCount - that.getClickCount != 0) return (clickCount - that.getClickCount).toInt
    else if (orderCount - that.getOrderCount != 0) return (orderCount - that.getOrderCount).toInt
    else if (payCount - that.getPayCount != 0) return (payCount - that.getPayCount).toInt
    0
  }

  def getClickCount: Long = clickCount

  def setClickCount(clickCount: Long): Unit = {
    this.clickCount = clickCount
  }

  def getOrderCount: Long = orderCount

  def setOrderCount(orderCount: Long): Unit = {
    this.orderCount = orderCount
  }

  def getPayCount: Long = payCount

  def setPayCount(payCount: Long): Unit = {
    this.payCount = payCount
  }
}
