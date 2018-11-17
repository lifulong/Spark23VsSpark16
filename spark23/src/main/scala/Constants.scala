
object Constants {
//    val SessionPath = "hdfs:///test/ferrari/20181113/session_10w"
//    val ProductPath = "hdfs:///test/ferrari/20181113/product_10w"
//    val UserPath = "hdfs:///test/ferrari/20181113/user_10w"
//  val SessionPath = "hdfs:///test/ferrari/20181113/session_100w"
//  val ProductPath = "hdfs:///test/ferrari/20181113/product_100w"
//  val UserPath = "hdfs:///test/ferrari/20181113/user_100w"
  val SessionPath = "hdfs:///test/ferrari/20181113/session_1000w"
  val ProductPath = "hdfs:///test/ferrari/20181113/product_1000w"
  val UserPath = "hdfs:///test/ferrari/20181113/user_1000w"
//  val SessionPath = "/Users/moses/data/performance/session_10w"
//  val ProductPath = "/Users/moses/data/performance/product_10w"
//  val UserPath = "/Users/moses/data/performance/user_10w"

  val FIELD_SESSION_ID = "sessionid"
  val FIELD_SEARCH_KEYWORDS = "searchKeywords"
  val FIELD_CLICK_CATEGORY_IDS = "clickCategoryIds"
  val FIELD_AGE = "age"
  val FIELD_PROFESSIONAL = "professional"
  val FIELD_CITY = "city"
  val FIELD_SEX = "sex"
  val FIELD_VISIT_LENGTH = "visitLength"
  val FIELD_STEP_LENGTH = "stepLength"
  val FIELD_START_TIME = "startTime"
  val FIELD_CLICK_COUNT = "clickCount"
  val FIELD_ORDER_COUNT = "orderCount"
  val FIELD_PAY_COUNT = "payCount"
  val FIELD_CATEGORY_ID = "categoryid"

  val SESSION_COUNT = "session_count"

  val TIME_PERIOD_1s_3s = "1s_3s"
  val TIME_PERIOD_4s_6s = "4s_6s"
  val TIME_PERIOD_7s_9s = "7s_9s"
  val TIME_PERIOD_10s_30s = "10s_30s"
  val TIME_PERIOD_30s_60s = "30s_60s"
  val TIME_PERIOD_1m_3m = "1m_3m"
  val TIME_PERIOD_3m_10m = "3m_10m"
  val TIME_PERIOD_10m_30m = "10m_30m"
  val TIME_PERIOD_30m = "30m"

  val STEP_PERIOD_1_3 = "1_3"
  val STEP_PERIOD_4_6 = "4_6"
  val STEP_PERIOD_7_9 = "7_9"
  val STEP_PERIOD_10_30 = "10_30"
  val STEP_PERIOD_30_60 = "30_60"
  val STEP_PERIOD_60 = "60"

  val PARAM_START_DATE = "startDate"
  val PARAM_END_DATE = "endDate"
  val PARAM_START_AGE = "startAge"
  val PARAM_END_AGE = "endAge"
  val PARAM_PROFESSIONALS = "professionals"
  val PARAM_CITIES = "cities"
  val PARAM_SEX = "sex"
  val PARAM_KEYWORDS = "keywords"
  val PARAM_CATEGORY_IDS = "categoryIds"
  val PARAM_TARGET_PAGE_FLOW = "targetPageFlow"

  val USER_VISIT_ACTION_DATE = 0
  val USER_VISIT_ACTION_USER_ID = 1
  val USER_VISIT_ACTION_SESSION_ID = 2
  val USER_VISIT_ACTION_PAGE_ID = 3
  val USER_VISIT_ACTION_ACTION_TIME = 4
  val USER_VISIT_ACTION_SEARCH_KEYWORD = 5
  val USER_VISIT_ACTION_CLICK_CATEGORY_ID = 6
  val USER_VISIT_ACTION_CLICK_PRODUCT_ID = 7
  val USER_VISIT_ACTION_ORDER_CATEGORY_IDS = 8
  val USER_VISIT_ACTION_ORDER_PRODUCT_IDS = 9
  val USER_VISIT_ACTION_PAY_CATEGORY_IDS = 10
  val USER_VISIT_ACTION_PAY_PRODUCT_IDS = 11
  val USER_VISIT_ACTION_CITY_ID = 12

  val USER_INFO_USER_ID = 0
  val USER_INFO_USERNAME = 1
  val USER_INFO_NAME = 2
  val USER_INFO_AGE = 3
  val USER_INFO_PROFESSIONAL = 4
  val USER_INFO_CITY = 5
  val USER_INFO_SEX = 6

  val PRODUCT_INFO_PRODUCT_ID = 0
  val PRODUCT_INFO_PRODUCT_NAME = 1
  val PRODUCT_INFO_EXTEND_INFO = 2
}
