import java.text.{ParseException, SimpleDateFormat}
import java.util.{Calendar, Date}

object DateUtils {
  val TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd")
  val DATEKEY_FORMAT = new SimpleDateFormat("yyyyMMdd")

  /**
    * 判断一个时间是否在另一个时间之前
    *
    * @param time1 第一个时间
    * @param time2 第二个时间
    * @return 判断结果
    */
  def before(time1: String, time2: String): Boolean = {
    try {
      val dateTime1 = TIME_FORMAT.parse(time1)
      val dateTime2 = TIME_FORMAT.parse(time2)
      if (dateTime1.before(dateTime2)) return true
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    false
  }

  /**
    * 判断一个时间是否在另一个时间之后
    *
    * @param time1 第一个时间
    * @param time2 第二个时间
    * @return 判断结果
    */
  def after(time1: String, time2: String): Boolean = {
    try {
      val dateTime1 = TIME_FORMAT.parse(time1)
      val dateTime2 = TIME_FORMAT.parse(time2)
      if (dateTime1.after(dateTime2)) return true
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    false
  }

  /**
    * 计算时间差值（单位为秒）
    *
    * @param time1 时间1
    * @param time2 时间2
    * @return 差值
    */
  def minus(time1: String, time2: String): Int = {
    try {
      val datetime1 = TIME_FORMAT.parse(time1)
      val datetime2 = TIME_FORMAT.parse(time2)
      val millisecond = datetime1.getTime - datetime2.getTime
      return Integer.valueOf(String.valueOf(millisecond / 1000))
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    0
  }

  /**
    * 获取年月日和小时
    *
    * @param datetime 时间（yyyy-MM-dd HH:mm:ss）
    * @return 结果（yyyy-MM-dd_HH）
    */
  def getDateHour(datetime: String): String = {
    val date = datetime.split(" ")(0)
    val hourMinuteSecond = datetime.split(" ")(1)
    val hour = hourMinuteSecond.split(":")(0)
    date + "_" + hour
  }

  /**
    * 获取当天日期（yyyy-MM-dd）
    *
    * @return 当天日期
    */
  def getTodayDate: String = DATE_FORMAT.format(new Date)

  /**
    * 获取昨天的日期（yyyy-MM-dd）
    *
    * @return 昨天的日期
    */
  def getYesterdayDate: String = {
    val cal = Calendar.getInstance
    cal.setTime(new Date)
    cal.add(Calendar.DAY_OF_YEAR, -1)
    val date = cal.getTime
    DATE_FORMAT.format(date)
  }

  /**
    * 格式化日期（yyyy-MM-dd）
    *
    * @param date Date对象
    * @return 格式化后的日期
    */
  def formatDate(date: Date): String = DATE_FORMAT.format(date)

  /**
    * 格式化时间（yyyy-MM-dd HH:mm:ss）
    *
    * @param date Date对象
    * @return 格式化后的时间
    */
  def formatTime(date: Date): String = TIME_FORMAT.format(date)

  /**
    * 解析时间字符串
    *
    * @param time 时间字符串
    * @return Date
    */
  def parseTime(time: String): Date = {
    try
      return TIME_FORMAT.parse(time)
    catch {
      case e: ParseException =>
        e.printStackTrace()
    }
    null
  }

  /**
    * 格式化日期key
    *
    * @param date
    * @return
    */
  def formatDateKey(date: Date): String = DATEKEY_FORMAT.format(date)

  /**
    * 格式化日期key
    *
    * @param datekey
    * @return
    */
  def parseDateKey(datekey: String): Date = {
    try
      return DATEKEY_FORMAT.parse(datekey)
    catch {
      case e: ParseException =>
        e.printStackTrace()
    }
    null
  }

  /**
    * 格式化时间，保留到分钟级别
    * yyyyMMddHHmm
    *
    * @param date
    * @return
    */
  def formatTimeMinute(date: Date): String = {
    val sdf = new SimpleDateFormat("yyyyMMddHHmm")
    sdf.format(date)
  }

}
