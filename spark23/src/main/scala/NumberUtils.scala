import java.math.BigDecimal

object NumberUtils {
  /**
    * 格式化小数
    *
    * @param num   浮点数
    * @param scale 四舍五入的位数
    * @return 格式化小数
    */
  def formatDouble(num: Double, scale: Int): Double = {
    val bd = new BigDecimal(num)
    bd.setScale(scale, BigDecimal.ROUND_HALF_UP).doubleValue
  }
}
