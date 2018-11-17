import org.apache.spark.{AccumulatorParam}


object SessionAggrStatAccumulator extends AccumulatorParam[String] {

  def zero(init: String): String = {
    init
    //Constants.SESSION_COUNT + "=0|" + Constants.TIME_PERIOD_1s_3s + "=0|" + Constants.TIME_PERIOD_4s_6s + "=0|" + Constants.TIME_PERIOD_7s_9s + "=0|" + Constants.TIME_PERIOD_10s_30s + "=0|" + Constants.TIME_PERIOD_30s_60s + "=0|" + Constants.TIME_PERIOD_1m_3m + "=0|" + Constants.TIME_PERIOD_3m_10m + "=0|" + Constants.TIME_PERIOD_10m_30m + "=0|" + Constants.TIME_PERIOD_30m + "=0|" + Constants.STEP_PERIOD_1_3 + "=0|" + Constants.STEP_PERIOD_4_6 + "=0|" + Constants.STEP_PERIOD_7_9 + "=0|" + Constants.STEP_PERIOD_10_30 + "=0|" + Constants.STEP_PERIOD_30_60 + "=0|" + Constants.STEP_PERIOD_60 + "=0"
  }
  /**
    * 加操作
    *
    * @param v
    */
  override def addAccumulator(v1:String, v: String): String = {
    var result:String = ""
    val v2 = v
    //在result里找到相对应的字段加1
    if (StringUtils.isNotEmpty(v1) && StringUtils.isNotEmpty(v2)) {
      val oldValue = StringUtils.getFieldFromConcatString(v1, "\\|", v2)
      if (oldValue != null) {
        val newValue = Integer.valueOf(oldValue) + 1
        result = StringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue))
      }
    }
    result
  }

  /**
    * 合并两个AccumulatorV2
    *
    * @param other
    */
  def addInPlace(base: String, other: String): String = {
    var result:String = base
    if (other == null) return result
    val myFields = result.split("\\|")
    val otherFields = other.split("\\|")
    //对每个字段对应的值相加，并把新值赋给result
    var i = 0
    while (i < myFields.length) {
      val myValue = myFields(i).split("=")(1)
      val otherValue = otherFields(i).split("=")(1)
      if (StringUtils.isNotEmpty(myValue) && StringUtils.isNotEmpty(otherValue)) {
        val newValue = Integer.valueOf(myValue) + Integer.valueOf(otherValue)
        result = StringUtils.setFieldInConcatString(result, "\\|", myFields(i).split("=")(0), String.valueOf(newValue))
      }
      i = i+1
    }
    result
  }

}
