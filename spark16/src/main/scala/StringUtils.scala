import scala.util.control.Breaks

object StringUtils extends Serializable {

  /**
    * 判断字符串是否为空
    *
    * @param str 字符串
    * @return 是否为空
    */
  def isEmpty(str: String): Boolean = str == null || "" == str

  /**
    * 判断字符串是否不为空
    *
    * @param str 字符串
    * @return 是否不为空
    */
  def isNotEmpty(str: String): Boolean = str != null && !("" == str)

  /**
    * 截断字符串两侧的逗号
    *
    * @param str 字符串
    * @return 字符串
    */
  def trimComma(str: String): String = {
    var newStr = str
    if (str.startsWith(",")) newStr = str.substring(1)
    if (str.endsWith(",")) newStr = str.substring(0, str.length - 1)
    newStr
  }

  /**
    * 补全两位数字
    *
    * @param str
    * @return
    */
  def fulfuill(str: String): String = if (str.length == 2) str
  else "0" + str

  /**
    * 从拼接的字符串中提取字段
    *
    * @param str       字符串
    * @param delimiter 分隔符
    * @param field     字段
    * @return 字段值
    */
  def getFieldFromConcatString(str: String, delimiter: String, field: String): String = {
    try {
      val fields = str.split(delimiter)
      for (concatField <- fields) { // searchKeywords=|clickCategoryIds=1,2,3
        if (concatField.split("=").length == 2) {
          val fieldName = concatField.split("=")(0)
          val fieldValue = concatField.split("=")(1)
          if (fieldName == field) return fieldValue
        }
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    null
  }

  /**
    * 从拼接的字符串中给字段设置值
    *
    * @param str           字符串
    * @param delimiter     分隔符
    * @param field         字段名
    * @param newFieldValue 新的field值
    * @return 字段值
    */
  def setFieldInConcatString(str: String, delimiter: String, field: String, newFieldValue: String): String = {
    val fields = str.split(delimiter)
    var i = 0
    val loop = new Breaks
    loop.breakable {
      while (i < fields.length) {
        val fieldName = fields(i).split("=")(0)
        if (fieldName == field) {
          val concatField = fieldName + "=" + newFieldValue
          fields(i) = concatField
          loop.break
        }
        i = i+1
      }
    }
    i = 0
    val buffer = new StringBuffer("")
    while (i < fields.length) {
      buffer.append(fields(i))
      if (i < fields.length - 1) buffer.append("|")
      i = i+1
    }
    buffer.toString
  }
}
