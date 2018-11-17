import java.io.Serializable
import java.util
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{Accumulator, AccumulatorParam, SparkConf, SparkContext}
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.ints.IntList
import org.apache.spark.storage.StorageLevel

import scala.util.Random
import scala.util.control.Breaks

object UserVisitSessionAnalyzeSpark16 extends Serializable {

  val master = "local"
  //val master = "yarn-cluster"
  val appName = "UserVisitSessionAnalyzeSpark16"
  val sparkConf = new SparkConf().setAppName(appName)//.setMaster(master)
  val spark = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(spark)
  val taskParams = new TaskParam(0,100,3,4,"",5,6)
  val taskId = 1

  val initValue = Constants.SESSION_COUNT + "=0|" + Constants.TIME_PERIOD_1s_3s + "=0|" + Constants.TIME_PERIOD_4s_6s + "=0|" + Constants.TIME_PERIOD_7s_9s + "=0|" + Constants.TIME_PERIOD_10s_30s + "=0|" + Constants.TIME_PERIOD_30s_60s + "=0|" + Constants.TIME_PERIOD_1m_3m + "=0|" + Constants.TIME_PERIOD_3m_10m + "=0|" + Constants.TIME_PERIOD_10m_30m + "=0|" + Constants.TIME_PERIOD_30m + "=0|" + Constants.STEP_PERIOD_1_3 + "=0|" + Constants.STEP_PERIOD_4_6 + "=0|" + Constants.STEP_PERIOD_7_9 + "=0|" + Constants.STEP_PERIOD_10_30 + "=0|" + Constants.STEP_PERIOD_30_60 + "=0|" + Constants.STEP_PERIOD_60 + "=0"


  def main(args: Array[String]): Unit = {
    val sessionRdd = getSessionInfo(Constants.SessionPath).rdd
    val actionRdd = getActionRddByRange(sessionRdd, "", "")
    val sessionid2ActionRDD = getSessionId2ActionRDD(actionRdd)
    val sessionid2AggrInfoRDD = aggregateBySession
    //spark.register(sessionAggrStatAccumulator) //TODO

    val sessionAggrStatAccumulator = spark.accumulator(initValue)(SessionAggrStatAccumulator)

    //按时间比例随机抽取并输出
    randomExtractSession(taskId, spark, sessionid2AggrInfoRDD, sessionid2ActionRDD)

    //过滤
    val filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(sessionid2AggrInfoRDD, taskParams, sessionAggrStatAccumulator)

    var sessionid2DetailRDD = getSessionid2DetailRDD(filteredSessionid2AggrInfoRDD, sessionid2ActionRDD)
    sessionid2DetailRDD = sessionid2DetailRDD.persist(StorageLevel.MEMORY_ONLY)

    val top10CategoryList = getTop10Category(taskId, sessionid2DetailRDD)

    getTop10Session(spark, taskId, top10CategoryList.toList, sessionid2DetailRDD)

    /**
      * 对于Accumulator中的数据输出 ！！！一定要有action操作，并且是放在前面
      */
    println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++AggrStat:")
    calculateAndPersistAggrStat(sessionAggrStatAccumulator.toString, taskId)

    spark.stop()
  }

  def getSessionInfo(path: String): DataFrame = {
    sqlContext.read.parquet(path)
  }

  def getProductInfo(path: String): DataFrame = {
    sqlContext.read.parquet(path)
  }

  def getUserInfo(path: String): DataFrame = {
    sqlContext.read.parquet(path)
  }

  def getActionRddByRange(session: RDD[Row], start: String, end: String): RDD[Row] = {
    session.filter(row => row.getString(0) >= "2018-01-03").filter(row => row.getString(0) <= "2018-02-03")
  }

  def getSessionId2ActionRDD(actionRdd: RDD[Row]): RDD[(String,Row)] = {
    actionRdd.mapPartitions{
      iterator => {
        var list = List[(String,Row)]()
        while(iterator.hasNext) {
          var row = iterator.next()
          list = (row.getString(2),row) +: list
        }
        list.iterator
      }
    }
  }

  def getUserId2FullInfoRDD(userRDD: RDD[Row]): RDD[(Int, Row)] ={
    userRDD.map(row => (row.getInt(0), row))
  }

  def getUserId2PartAggrInfoRDD(actionRDD: RDD[Row]): RDD[(Int, String)] = {
    val session2ActionRdd = actionRDD.map(row => (row.getString(2), row))
    session2ActionRdd.groupByKey().map{
      (tuple) => {
        val sessionId = tuple._1
        val iterator = tuple._2.iterator

        var userId:Int = -1
        var startTime:Date = null
        var endTime:Date = null
        var stepLength = 0

        val searchKeywordsBuffer = new StringBuilder
        val clickCategoryIdBuffer = new StringBuilder

        while(iterator.hasNext) {
          val row = iterator.next()
          if(userId == -1)
            userId = row.getInt(1)
          var searchKeywords = row.getString(5)
          var clickCategoryId = row.getInt(6).toString
          searchKeywordsBuffer ++= searchKeywords
          clickCategoryIdBuffer ++= clickCategoryId
          val timestamp = DateUtils.parseTime(row.getString(4)) //row.getTimestamp(4)
          if(startTime == null || timestamp.before(startTime))
            startTime = timestamp
          if(endTime==null || timestamp.after(endTime))
            endTime = timestamp
          stepLength = stepLength + 1
        }
        val visitLength = (endTime.getTime - startTime.getTime)/1000
        val searchKeyWords = searchKeywordsBuffer.mkString(",")
        val clickCategoryIds = clickCategoryIdBuffer.mkString(",")
        val partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeyWords + "|" + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|" + Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" + Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" + Constants.FIELD_START_TIME + "=" + startTime
        (userId, partAggrInfo)
      }
    }
  }

  def getSessionId2FullInfoRDD(userId2PartAggrInfoRDD: RDD[(Int,String)], userFullInfoRDD:RDD[(Int,Row)]): RDD[(String,String)] = {
    userId2PartAggrInfoRDD.join(userFullInfoRDD).map{
      (tuple) => {
        var partAggrInfo = tuple._2._1
        var userInfoRow = tuple._2._2
        val sessionid = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID)

        val age = userInfoRow.getInt(3)
        val professional = userInfoRow.getInt(4).toString
        val city = userInfoRow.getInt(5).toString
        val sex = userInfoRow.getString(6)

        val fullAggrInfo = partAggrInfo + "|" + Constants.FIELD_AGE + "=" + age + "|" + Constants.FIELD_PROFESSIONAL + "=" + professional + "|" + Constants.FIELD_CITY + "=" + city + "|" + Constants.FIELD_SEX + "+" + sex + "|"

        (sessionid, fullAggrInfo)
      }
    }
  }

  def aggregateBySession(): RDD[(String,String)] = {
    val sessionRdd = getSessionInfo(Constants.SessionPath).rdd
    val userRdd = getUserInfo(Constants.UserPath).rdd
    val actionRdd = getActionRddByRange(sessionRdd, "", "")
    val userId2FullInfoRdd = getUserId2FullInfoRDD(userRdd)
    val userId2PartAggrInfoRdd = getUserId2PartAggrInfoRDD(actionRdd)
    getSessionId2FullInfoRDD(userId2PartAggrInfoRdd, userId2FullInfoRdd)
  }

  def filterSessionAndAggrStat(sessionid2AggrInfoRDD: RDD[(String, String)], taskParam: TaskParam, sessionAggrStatAccumulator: Accumulator[String]) = {
    val startAge = 0
    val endAge = 100
    val professional = null
    val cities = null
    val sex = null
    val keywords = null
    val categoryIds = null
    var _parameter = (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|"
      else "") + (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|"
      else "") + (if (professional != null) Constants.PARAM_PROFESSIONALS + "=" + professional + "|"
      else "") + (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|"
      else "") + (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|"
      else "") + (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|"
      else "") + (if (categoryIds != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds + "|"
      else "")
    if (_parameter.endsWith("\\|")) _parameter = _parameter.substring(0, _parameter.length - 1)
    val parameter = _parameter

    /**
      * 对session的时间范围进行累加
      *
      * @param visitLength
      */
    val calculateVisitLength = (visitLength: Long) => {
      if (visitLength >= 1 && visitLength <= 3) sessionAggrStatAccumulator += Constants.TIME_PERIOD_1s_3s
      else if (visitLength >= 4 && visitLength <= 6) sessionAggrStatAccumulator += Constants.TIME_PERIOD_4s_6s
      else if (visitLength >= 7 && visitLength <= 9) sessionAggrStatAccumulator += Constants.TIME_PERIOD_7s_9s
      else if (visitLength >= 10 && visitLength <= 30) sessionAggrStatAccumulator += Constants.TIME_PERIOD_10s_30s
      else if (visitLength > 30 && visitLength <= 60) sessionAggrStatAccumulator += Constants.TIME_PERIOD_30s_60s
      else if (visitLength > 60 && visitLength <= 180) sessionAggrStatAccumulator += Constants.TIME_PERIOD_1m_3m
      else if (visitLength > 180 && visitLength <= 600) sessionAggrStatAccumulator += Constants.TIME_PERIOD_3m_10m
      else if (visitLength > 600 && visitLength <= 1800) sessionAggrStatAccumulator += Constants.TIME_PERIOD_10m_30m
      else if (visitLength > 1800) sessionAggrStatAccumulator += Constants.TIME_PERIOD_30m
    }

    /**
      * 对session的步长进行累加
      *
      * @param stepLength
      */
    val calculateStepLength = (stepLength: Long) => {
      if (stepLength >= 1 && stepLength <= 3) sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3)
      else if (stepLength >= 4 && stepLength <= 6) sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6)
      else if (stepLength >= 7 && stepLength <= 9) sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9)
      else if (stepLength >= 10 && stepLength <= 30) sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30)
      else if (stepLength > 30 && stepLength <= 60) sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60)
      else if (stepLength > 60) sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60)
    }

    val filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(
      (tuple) => {
        var flag = true
        val aggrInfo = tuple._2
        //按照年龄范围进行过滤
        if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) flag = false
        //按照职业范围进行过滤
        if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, parameter, Constants.PARAM_PROFESSIONALS)) flag = false
        //按照城市范围进行过滤
        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, parameter, Constants.PARAM_CITIES)) flag = false
        //按照性别进行过滤
        if (!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX, parameter, Constants.PARAM_SEX)) flag = false
        // 按照搜索词进行过滤
        if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS)) flag = false
        //按照点击品类id进行过滤
        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter, Constants.PARAM_CATEGORY_IDS)) flag = false
        //过滤完了之后要对session的访问时长和访问步长进行统计
        sessionAggrStatAccumulator.add(Constants.SESSION_COUNT)
        //计算出session的访问时长和访问步长的范围，并进行相应的累加
        val visitLength = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
        val stepLength = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong
        calculateVisitLength(visitLength)
        calculateStepLength(stepLength)
        flag
      }
    )
    filteredSessionid2AggrInfoRDD
  }

  /**
    * 随机抽取session数据
    *
    * @param sessionid2AggrInfoRDD
    */
  def randomExtractSession(taskid: Int, spark: SparkContext, sessionid2AggrInfoRDD: RDD[(String, String)], sessionid2ActionRDD: RDD[(String, Row)]): Unit = { //得到<yyyy-MM-dd_HH, aggrInfo>
    val time2AggrInfoRDD = sessionid2AggrInfoRDD.map(
      (tuple2) => {
        val aggrInfo = tuple2._2
        //System.out.println("randomExtractSession aggrInfo: " + aggrInfo);
        val startTime = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_START_TIME)
        //System.out.println("randomExtractSession startTime" + startTime);
        // dateHour: yyyy-MM-dd_HH
        val dateHour = DateUtils.getDateHour(startTime)
        new Tuple2[String, String](dateHour, aggrInfo)
      }
    )
    //计算每天每小时的session数量
    val countMap = time2AggrInfoRDD.countByKey
    //按时间比例随机抽取,计算出每天每小时要抽取session的索引
    //将<yyyy-MM-dd_HH, count>转变为<yyyy-MM-dd, <HH, count>>
    val dateHourCountMap = new util.HashMap[String, util.HashMap[String, Integer]]
    import scala.collection.JavaConversions._
    for (countEntry <- countMap.entrySet) {
      val dateHour = countEntry.getKey
      val date = dateHour.split("_")(0)
      val hour = dateHour.split("_")(1)
      val count = countEntry.getValue
      var hourCountMap = dateHourCountMap.get(date)
      if (hourCountMap == null) {
        hourCountMap = new util.HashMap[String, Integer]
        dateHourCountMap.put(date, hourCountMap)
      }
      hourCountMap.put(hour, count.toInt)
    }
    //总共要抽取100个session，先按照天数，进行平分
    val extractNumberPerDay = 100 / dateHourCountMap.size
    //<date, <hour, (3,5,20,102)>>
    val dateHourExtractMap = new util.HashMap[String, util.HashMap[String, util.ArrayList[Integer]]]
    val random = new Random
    import scala.collection.JavaConversions._
    for (dateHourCountEntry <- dateHourCountMap.entrySet) {
      val date = dateHourCountEntry.getKey
      val hourCountMap = dateHourCountEntry.getValue
      var hourExtractMap = dateHourExtractMap.get(date)
      if (hourExtractMap == null) {
        hourExtractMap = new util.HashMap[String, util.ArrayList[Integer]]
        dateHourExtractMap.put(date, hourExtractMap)
      }
      //计算出这一天的session总数
      var sessionCount = 0L
      import scala.collection.JavaConversions._
      for (hourCount <- hourCountMap.values) {
        sessionCount += hourCount
      }
      //遍历小时
      import scala.collection.JavaConversions._
      for (hourCountEntry <- hourCountMap.entrySet) {
        val hour = hourCountEntry.getKey
        val count = hourCountEntry.getValue
        //计算每个小时的session的数量占据当天session数量的比例,然后得到每个小时要抽取的数量
        var hourExtractNumber = ((count.toDouble / sessionCount.toDouble) * extractNumberPerDay).toInt
        if (hourExtractNumber > count) hourExtractNumber = count.toInt
        //得到当前小时存放的随机索引List
        var extractIndexList = hourExtractMap.get(hour)
        if (extractIndexList == null) {
          extractIndexList = new util.ArrayList[Integer]
          hourExtractMap.put(hour, extractIndexList)
        }
        //TODO 随机生成索引，效率太低
        var i = 0
        while (i < hourExtractNumber) {
          var extractIndex = random.nextInt(count.toInt)
          while (extractIndexList.contains(extractIndex))
            extractIndex = random.nextInt(count.toInt)
          extractIndexList.add(extractIndex)
          i = i+1
        }
      }
    }
    /**
      * fastutil的使用
      *
      */
    val fastutilDateHourExtractMap = new util.HashMap[String, util.HashMap[String, IntList]]
    import scala.collection.JavaConversions._
    for (dateHourExtractEntry <- dateHourExtractMap.entrySet) {
      val date = dateHourExtractEntry.getKey
      val hourExtractMap = dateHourExtractEntry.getValue
      val fastutilHourExtractMap = new util.HashMap[String, IntList]
      import scala.collection.JavaConversions._
      for (hourExtractEntry <- hourExtractMap.entrySet) {
        val hour = hourExtractEntry.getKey
        val extractList = hourExtractEntry.getValue
        val fastutilExtractList = new IntArrayList
        var i = 0
        while (i < extractList.size) {
          fastutilExtractList.add(extractList.get(i))
          i = i + 1
        }
        fastutilHourExtractMap.put(hour, fastutilExtractList)
      }
      fastutilDateHourExtractMap.put(date, fastutilHourExtractMap)
    }
    /**
      * 广播变量
      * 这个map有点大，为了避免每个task都拷贝一份这个map，就把它设置成广播变量
      */
    val sparkContext = spark
    val dateHourExtractMapBroadcast = sparkContext.broadcast(fastutilDateHourExtractMap)
    //根据随机索引进行抽取
    val time2session2RDD = time2AggrInfoRDD.groupByKey
    val extractSessionidsRDD = time2session2RDD.flatMap(
      (tuple2) => { //存放返回值的list，使用tuple的原因是方便join
        val extractSessionids = new util.ArrayList[Tuple2[String, String]]
        val dateHour = tuple2._1
        val date = dateHour.split("_")(0)
        val hour = dateHour.split("_")(1)
        val iterator = tuple2._2.iterator
        /**
          * 使用广播变量的时候，直接调用广播变量的value方法
          */
        val dateHourExtractMap = dateHourExtractMapBroadcast.value
        val extractIndexList = dateHourExtractMap.get(date).get(hour)
        var index = 0
        while (iterator.hasNext) {
          val aggrInfo = iterator.next
          val sessionid = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SESSION_ID)
          if (extractIndexList.contains(index)) {
            val sessionRandomExtract = new SessionRandomExtract(
              taskid,
              sessionid,
              StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_START_TIME),
              StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS),
              StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)
            )
            print(sessionRandomExtract)
            //将sessionid放入list
            extractSessionids.add(new Tuple2[String, String](sessionid, sessionid))
          }
          index += 1
        }
        //需要返回iterator
        extractSessionids.iterator
      }
    )
    /**
      * 第四步，获取抽取出来的session的明细数据打印输出
      */
    val extractSessionDetailRDD = extractSessionidsRDD.join(sessionid2ActionRDD)
    extractSessionDetailRDD.foreach{
      (tuple2)  => {
        val row = tuple2._2._2
        val sessionDetail = new SessionDetail(
          taskid,
          row.getInt(Constants.USER_VISIT_ACTION_USER_ID),
          row.getString(Constants.USER_VISIT_ACTION_SESSION_ID),
          row.getInt(Constants.USER_VISIT_ACTION_PAGE_ID),
          row.getString(Constants.USER_VISIT_ACTION_ACTION_TIME),
          row.getString(Constants.USER_VISIT_ACTION_SEARCH_KEYWORD),
          row.getInt(Constants.USER_VISIT_ACTION_CLICK_CATEGORY_ID),
          row.getInt(Constants.USER_VISIT_ACTION_CLICK_PRODUCT_ID),
          row.getInt(Constants.USER_VISIT_ACTION_ORDER_CATEGORY_IDS).toString,
          row.getInt(Constants.USER_VISIT_ACTION_ORDER_PRODUCT_IDS).toString,
          row.getInt(Constants.USER_VISIT_ACTION_PAY_CATEGORY_IDS).toString,
          row.getInt(Constants.USER_VISIT_ACTION_PAY_PRODUCT_IDS).toString
        )
        print(sessionDetail)
      }
    }
  }

  /**
    * 计算占比并打印输出
    *
    * @param value
    * @param taskid
    */
  def calculateAndPersistAggrStat(value: String, taskid: Int): Unit = {
    val session_count = StringUtils.getFieldFromConcatString(value, "\\|", Constants.SESSION_COUNT).toLong
    val visit_length_1s_3s = StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1s_3s).toDouble
    val visit_length_4s_6s = StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_4s_6s).toDouble
    val visit_length_7s_9s = StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_7s_9s).toDouble
    val visit_length_10s_30s = StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10s_30s).toDouble
    val visit_length_30s_60s = StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30s_60s).toDouble
    val visit_length_1m_3m = StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1m_3m).toDouble
    val visit_length_3m_10m = StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1m_3m).toDouble
    val visit_length_10m_30m = StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10m_30m).toDouble
    val visit_length_30m = StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30m).toDouble
    val step_length_1_3 = StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_1_3).toDouble
    val step_length_4_6 = StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_4_6).toDouble
    val step_length_7_9 = StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_7_9).toDouble
    val step_length_10_30 = StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_10_30).toDouble
    val step_length_30_60 = StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_30_60).toDouble
    val step_length_60 = StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_60).toDouble
    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s.toDouble / session_count.toDouble, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s.toDouble / session_count.toDouble, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s.toDouble / session_count.toDouble, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s.toDouble / session_count.toDouble, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s.toDouble / session_count.toDouble, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m.toDouble / session_count.toDouble, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m.toDouble / session_count.toDouble, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m.toDouble / session_count.toDouble, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m.toDouble / session_count.toDouble, 2)
    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3.toDouble / session_count.toDouble, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6.toDouble / session_count.toDouble, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9.toDouble / session_count.toDouble, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30.toDouble / session_count.toDouble, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60.toDouble / session_count.toDouble, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60.toDouble / session_count.toDouble, 2)
    //将统计结果封装称domain
    val sessionAggrStat = new SessionAggrStat(
      taskid,
      session_count,
      visit_length_1s_3s_ratio,
      visit_length_4s_6s_ratio,
      visit_length_7s_9s_ratio,
      visit_length_10s_30s_ratio,
      visit_length_30s_60s_ratio,
      visit_length_1m_3m_ratio,
      visit_length_3m_10m_ratio,
      visit_length_10m_30m_ratio,
      visit_length_30m_ratio,
      step_length_1_3_ratio,
      step_length_4_6_ratio,
      step_length_7_9_ratio,
      step_length_10_30_ratio,
      step_length_30_60_ratio,
      step_length_60_ratio
    )
    print(sessionAggrStat)
  }


  /**
    * 得到符合条件的session的访问明细
    *
    * @param filteredSessionid2AggrInfoRDD
    * @param sessionid2ActionRDD
    * @return
    */
  def getSessionid2DetailRDD(filteredSessionid2AggrInfoRDD: RDD[(String, String)], sessionid2ActionRDD: RDD[(String, Row)]) = {
    val sessionid2DetailRDD = filteredSessionid2AggrInfoRDD.join(sessionid2ActionRDD).map( (tuple2) => {
      new Tuple2[String, Row](tuple2._1, tuple2._2._2)
    })
    sessionid2DetailRDD
  }

  /**
    * 获取top10热门品类
    *
    * @param taskid
    * @param sessionid2DetailRDD <sessionId,Row>
    * @return Array<category,category>
    */
  def getTop10Category(taskid: Int, sessionid2DetailRDD: RDD[(String, Row)]) = {
    /**
      * 第一步：获取符合条件的session访问过的所有品类
      * 获取session访问过的所有品类id，也就是点击过、下单过、支付过
      */
    import scala.collection.JavaConversions._
    var categoryidRDD = sessionid2DetailRDD.flatMap(
      (tuple2) => {
        val categoryList = new util.ArrayList[Tuple2[Long, Long]]
        val row = tuple2._2
        try {
          val clickCategoryId = row.getInt(Constants.USER_VISIT_ACTION_CLICK_CATEGORY_ID)
          if (clickCategoryId != null)
            categoryList.add(new Tuple2[Long, Long](clickCategoryId, clickCategoryId))
          val orderCategoryIds = row.getInt(Constants.USER_VISIT_ACTION_ORDER_CATEGORY_IDS).toString
          if (orderCategoryIds != null) {
            val orderCategoryIdSplited = orderCategoryIds.split(",")
            for (orderCategoryId <- orderCategoryIdSplited) {
              categoryList.add((orderCategoryId.toLong, orderCategoryId.toLong))
            }
          }
          val payCategoryIds = row.getInt(Constants.USER_VISIT_ACTION_PAY_CATEGORY_IDS).toString
          if (payCategoryIds != null) {
            val payCategoryIdSplited = payCategoryIds.split(",")
            for (payCategoryId <- payCategoryIdSplited) {
              categoryList.add((payCategoryId.toLong, payCategoryId.toLong))
            }
          }
        } catch {
          case e: Exception =>
        }
        categoryList.iterator
      }
    )

    /**
      * 一定要进行去重
      */
    categoryidRDD = categoryidRDD.distinct
    /**
      * 第二步：计算各品类的点击、下单和支付的次数
      */
    //
    val clickCategoryId2CountRDD = getClickCategoryId2CountRDD(sessionid2DetailRDD)
    val orderCategoryId2CountRDD = getOrderCategoryId2CountRDD(sessionid2DetailRDD)
    val payCategoryId2CountRDD = getPayCategoryId2CountRDD(sessionid2DetailRDD)
    /**
      * 第三步：join操作
      */
    val categoryid2CountRDD = joinCategoryAndData(categoryidRDD, clickCategoryId2CountRDD, orderCategoryId2CountRDD, payCategoryId2CountRDD)
    /**
      * 第四步：自定义二次排序key
      */
    /**
      * 第五步：将数据映射成<CategorySortKey, Info>格式的RD，然后进行二次排序
      */
    val sortKey2CountRDD = categoryid2CountRDD.map(
      (tuple2) => {
        val countInfo = tuple2._2
        val clickCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT).toLong
        val orderCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT).toLong
        val payCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT).toLong
        val sortKey = new CategorySortKey(clickCount, orderCount, payCount)
        new Tuple2[CategorySortKey, String](sortKey, countInfo)
      }
    )
    val sortedCategoryCountRDD = sortKey2CountRDD.sortByKey(false)
    /**
      * 第六步：用take(10)取出,并输出显示
      */
    val top10CategoryList = sortedCategoryCountRDD.take(10)
    for (tuple2 <- top10CategoryList) {
      val countInfo = tuple2._2
      val categoryId = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
      val clickCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT).toLong
      val orderCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT).toLong
      val payCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT).toLong
      val category = new Top10Category(taskid, categoryId, clickCount, orderCount, payCount)
      print(category)
    }
    top10CategoryList
  }

  /**
    * 获取各品类点击次数RDD
    *
    * @param sessionid2DetailRDD
    * @return
    */
  def getClickCategoryId2CountRDD(sessionid2DetailRDD: RDD[(String, Row)]) = { //过滤
    /**
      * 因为过滤出来的点击行为很少，所以过滤后每个paitition中的数据量很不均匀，而且很少
      * 所以可以使用coalesce
      */
    val clickCategoryId2CountRDD = sessionid2DetailRDD.filter(
      (tuple2) => {
        val row = tuple2._2
        val flag = if (row.get(Constants.USER_VISIT_ACTION_CLICK_CATEGORY_ID) != null) true else false
        flag
      }
    ).map(
      (tuple2) => {
        val clickCategoryId = tuple2._2.getInt(Constants.USER_VISIT_ACTION_CLICK_CATEGORY_ID).toLong
        new Tuple2[Long, Long](clickCategoryId, 1L)
      }
    ).reduceByKey((v1, v2) => v1 + v2)

    clickCategoryId2CountRDD
  }

  /**
    * 获取各品类下单次数RDD
    *
    * @param sessionid2DetailRDD
    * @return
    */
  def getOrderCategoryId2CountRDD(sessionid2DetailRDD: RDD[(String, Row)]) = {
    import scala.collection.JavaConversions._
    val orderActionRDD = sessionid2DetailRDD.filter(
      (tuple2) => {
        val row = tuple2._2
        val flag = if (row.get(Constants.USER_VISIT_ACTION_ORDER_CATEGORY_IDS) != null) true
        else false
        flag
      }
    )
    val orderCategoryIdRDD = orderActionRDD.flatMap(
      (tuple2) => {
        val orderCategoryIds = tuple2._2.getInt(Constants.USER_VISIT_ACTION_ORDER_CATEGORY_IDS).toString
        val orderCategoryIdsSplited = orderCategoryIds.split(",")
        val list = new util.ArrayList[Tuple2[Long, Long]]
        for (orderCategoryId <- orderCategoryIdsSplited) {
          list.add(new Tuple2[Long, Long](orderCategoryId.toLong, 1L))
        }
        list.iterator
      }
    )
    val orderCategoryId2CountRDD = orderCategoryIdRDD.reduceByKey((v1: Long, v2: Long) => v1 + v2)
    orderCategoryId2CountRDD
  }

  /**
    * 获取各品类支付次数RDD
    *
    * @param sessionid2DetailRDD
    * @return
    */
  def getPayCategoryId2CountRDD(sessionid2DetailRDD: RDD[(String, Row)]) = {
    val payActionRDD = sessionid2DetailRDD.filter(
      (tuple2) => {
        val row = tuple2._2
        val flag = if (row.get(Constants.USER_VISIT_ACTION_PAY_CATEGORY_IDS) != null) true
        else false
        flag
      }
    )
    import scala.collection.JavaConversions._
    val payCategoryIdRDD = payActionRDD.flatMap(
      (tuple2) => {
        val payCategoryIds = tuple2._2.getInt(Constants.USER_VISIT_ACTION_PAY_CATEGORY_IDS).toString
        val payCategoryIdsSplited = payCategoryIds.split(",")
        val list = new util.ArrayList[Tuple2[Long, Long]]
        for (payCategoryId <- payCategoryIdsSplited) {
          list.add(new Tuple2[Long, Long](payCategoryId.toLong, 1L))
        }
        list.iterator
      }
    )
    val payCategoryId2CountRDD = payCategoryIdRDD.reduceByKey((v1: Long, v2: Long) => v1 + v2)
    payCategoryId2CountRDD
  }

  def joinCategoryAndData(categoryidRDD: RDD[(Long, Long)], clickCategoryId2CountRDD: RDD[(Long, Long)], orderCategoryId2CountRDD: RDD[(Long, Long)], payCategoryId2CountRDD: RDD[(Long, Long)]) = {
    val tmpJoinRDD = categoryidRDD.leftOuterJoin(clickCategoryId2CountRDD)
    var tmpMapRDD = tmpJoinRDD.map(
      (tuple2) => {
        val categoryid = tuple2._1
        val optional = tuple2._2._2
        var clickCount = 0L
        if (optional.isDefined) clickCount = optional.get
        val value = Constants.FIELD_CATEGORY_ID + "=" + categoryid + "|" + Constants.FIELD_CLICK_COUNT + "=" + clickCount
        new Tuple2[Long, String](categoryid, value)
      }
    )
    tmpMapRDD = tmpMapRDD.leftOuterJoin(orderCategoryId2CountRDD).map( {
      (tuple2) => {
        val categoryid = tuple2._1
        var value = tuple2._2._1
        val optional = tuple2._2._2
        var orderCount = 0L
        if (optional.isDefined) orderCount = optional.get
        value = value + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount
        new Tuple2[Long, String](categoryid, value)
      }
    })
    tmpMapRDD = tmpMapRDD.leftOuterJoin(payCategoryId2CountRDD).map(
      (tuple2) => {
        val categoryid = tuple2._1
        var value = tuple2._2._1
        val optional = tuple2._2._2
        var payCount = 0L
        if (optional.isDefined) payCount = optional.get
        value = value + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount
        new Tuple2[Long, String](categoryid, value)
      }
    )
    tmpMapRDD
  }


  /**
    * 获取top10活跃session
    *
    * @param spark
    * @param taskId
    * @param top10CategoryList
    * @param sessionid2DetailRDD
    */
  def getTop10Session(spark: SparkContext, taskId: Int, top10CategoryList: List[Tuple2[CategorySortKey, String]], sessionid2DetailRDD: RDD[(String, Row)]): Unit = {
    /**
      * 第一步：将top10热门品类的id生成RDD
      */
    val top10CategoryIdList = new util.ArrayList[Tuple2[Long, Long]]
    import scala.collection.JavaConversions._
    for (tuple2 <- top10CategoryList) {
      val categoryid = StringUtils.getFieldFromConcatString(tuple2._2, "\\|", Constants.FIELD_CATEGORY_ID).toLong
      top10CategoryIdList.add(new Tuple2[Long, Long](categoryid, categoryid))
    }
    val sc = spark
    val top10CategoryIdRDD = sc.parallelize(top10CategoryIdList)
    /**
      * 第二步：计算top10品类被各session点击的次数
      */
    val sessionid2DetailsRDD = sessionid2DetailRDD.groupByKey
    val categoryid2SessionCountRDD = sessionid2DetailsRDD.flatMap(
      (tuple2) => {
        val sessionid = tuple2._1
        val iterator = tuple2._2.iterator
        val categoryCountMap = new util.HashMap[Long, Long]
        while (iterator.hasNext) {
          val row = iterator.next
          if (row.get(Constants.USER_VISIT_ACTION_CLICK_CATEGORY_ID) != null) {
            val categoryid = row.getInt(Constants.USER_VISIT_ACTION_CLICK_CATEGORY_ID).toLong
            var count = categoryCountMap.get(categoryid)
            if (count == null) count = 0L
            count += 1
            categoryCountMap.put(categoryid, count)
          }
        }
        val list = new util.ArrayList[Tuple2[Long, String]]
        import scala.collection.JavaConversions._
        for (categoryCountEntry <- categoryCountMap.entrySet) {
          val categoryid = categoryCountEntry.getKey
          val count = categoryCountEntry.getValue
          val value = sessionid + "," + count
          list.add(new Tuple2[Long, String](categoryid, value))
        }
        list.iterator
      }
    )
    //获取到top10热门品类被各个session点击的次数
    val top10CategorySessionCountRDD = top10CategoryIdRDD.join(categoryid2SessionCountRDD).map({
      (tuple2) => (tuple2._1, tuple2._2._2)
    })
    /**
      * 第三步：分组取TopN算法实现，获取每个品类的top10活跃用户
      */
    val top10CategorySessionCountsRDD = top10CategorySessionCountRDD.groupByKey
    val top10SessionRDD = top10CategorySessionCountsRDD.flatMap(
      (tuple2) => {
        val categoryid = tuple2._1
        val iterator = tuple2._2.iterator
        //定义取top10的排序数组
        val top10Sessions = new Array[String](3)
        val loop = new Breaks
        loop.breakable{
        while (iterator.hasNext) {
          val sessionCount = iterator.next
          val count = (sessionCount.split(",")(1)).toLong
          var i = 0
          while (i < top10Sessions.length) {
            if (top10Sessions(i) == null) {
              top10Sessions(i) = sessionCount
              loop.break
            }
            else {
              val _count = (top10Sessions(i).split(",")(1)).toLong
              if (count > _count) {
                var j = top10Sessions.length - 1
                while (j > i) {
                  top10Sessions(j) = top10Sessions(j - 1)
                  j = j - 1
                }
                top10Sessions(i) = sessionCount
                loop.break
              }
            }
            i = i+1
          }
        }
        }
        val list = new util.ArrayList[Tuple2[String, String]]
        for (sessionCount <- top10Sessions) {
          if (sessionCount != null) {
            val sessionid = sessionCount.split(",")(0)
            val count = sessionCount.split(",")(1).toLong
            val top10Session = new Top10Session(taskId, categoryid.toInt, sessionid, count)
            print(top10Session)
            list.add(new Tuple2[String, String](sessionid, sessionid))
          }
        }
        list.iterator
      }
    )
    /**
      * 第四步：获取top10活跃session的明细数据，并写入mysql
      */
    val sessionDetailRDD = top10SessionRDD.join(sessionid2DetailRDD)
    sessionDetailRDD.foreachPartition(
      (iterator) => {
        val sessionDetails = new util.ArrayList[SessionDetail]
        while ( {
          iterator.hasNext
        }) {
          val tuple2 = iterator.next
          val row = tuple2._2._2
          val sessionDetail = new SessionDetail(
            taskId,
            row.getInt(Constants.USER_VISIT_ACTION_USER_ID),
            row.getString(Constants.USER_VISIT_ACTION_SESSION_ID),
            row.getInt(Constants.USER_VISIT_ACTION_PAGE_ID),
            row.getString(Constants.USER_VISIT_ACTION_ACTION_TIME),
            row.getString(Constants.USER_VISIT_ACTION_SEARCH_KEYWORD),
            row.getInt(Constants.USER_VISIT_ACTION_CLICK_CATEGORY_ID),
            row.getInt(Constants.USER_VISIT_ACTION_CLICK_PRODUCT_ID),
            row.getInt(Constants.USER_VISIT_ACTION_ORDER_CATEGORY_IDS).toString,
            row.getInt(Constants.USER_VISIT_ACTION_ORDER_PRODUCT_IDS).toString,
            row.getInt(Constants.USER_VISIT_ACTION_PAY_CATEGORY_IDS).toString,
            row.getInt(Constants.USER_VISIT_ACTION_PAY_PRODUCT_IDS).toString
          )
          print(sessionDetails)
        }
      }
    )
  }
}



case class TaskParam(
                      val startAge: Int,
                      val endAge: Int,
                      val professional:Int,
                      val cities:Int,
                      val sex:String,
                      val keywords:Int,
                      val categoryIds:Int
                    )

case class Top10Category(val taskId:Long,
                         val categoryId:Long,
                         val clickCount:Long,
                         val orderCount:Long,
                         val payCount:Long
                        )

case class Top10Session(
                         val taskId:Long,
                         val categoryId:Int,
                         val sessionId:String,
                         val clickCount:Long
                       )

case class SessionDetail(
                          val taskId: Int,
                          val userId: Int,
                          val sessionId: String,
                          val pageId:Int,
                          val actionTime:String,
                          val searchKeywords:String,
                          val clickCategoryId:Int,
                          val clickProductId:Int,
                          val orderCategoryIds:String,
                          val orderProductIds:String,
                          val payCategoryIds:String,
                          val payProductIds:String
                        )

case class SessionRandomExtract(
                                 val taskId:Int,
                                 val sessionId:String,
                                 val startTime:String,
                                 val searchKeywords:String,
                                 val clickCategoryIds:String
                               )

class SessionAggrStat(
                       val taskId:Int,
                       val session_count:Long,
                       val visit_length_1s_3s_ratio:Double,
                       val visit_length_4s_6s_ratio:Double,
                       val visit_length_7s_9s_ratio:Double,
                       val visit_length_10s_30s_ratio:Double,
                       val visit_length_30s_60s_ratio:Double,
                       val visit_length_1m_3m_ratio:Double,
                       val visit_length_3m_10m_ratio:Double,
                       val visit_length_10m_30m_ratio:Double,
                       val visit_length_30m_ratio:Double,
                       val step_length_1_3_ratio:Double,
                       val step_length_4_6_ratio:Double,
                       val step_length_7_9_ratio:Double,
                       val step_length_10_30_ratio:Double,
                       val step_length_30_60_ratio:Double,
                       val step_length_60_ratio:Double
                     )

