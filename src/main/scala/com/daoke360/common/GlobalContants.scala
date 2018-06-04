package com.daoke360.common

object GlobalContants {
  /**
    * 默认值
    */
  final val DEFAULT_VALUE: String = "unknown"
  final val VALUE_OF_ALL: String = "all"


  /**
    * 日志分割标记
    */
  final val LOG_SPLIT_FLAG: String = "\\|"

  /**
    * 任务参数开始的标记
    */
  final val TASK_PARAMS_FLAG: String = "-d"

  /**
    * 任务运行日期
    */
  final val TASK_RUN_DATE = "task_run_date"

  /**
    * 任务id
    */
  final val TASK_RUN_ID = "task_run_id"

  /**
    * 日志存放根目录
    */
  final val LOG_DIR_PREFIX = "/logs/"
  /**
    * 任务的输入路径
    */
  final val TASK_INPUT_PATH: String = "task_input_path"

  /**
    * JDBC mysql 驱动
    */
  final val JDBC_DRIVER = "jdbc.driver"

  /**
    * 数据库连接池初始大小
    */
  final val JDBC_DATA_SOURCE_SIZE = "jdbc.datasource.size"
  /**
    * mysql url
    */
  final val JDBC_URL = "jdbc.url"
  /**
    * mysql 用户名
    */
  final val JDBC_USER = "jdbc.user"
  /**
    * mysql 密码
    */
  final val JDBC_PASSWORD = "jdbc.password"

  /**
    * 任务相关的常量
    */
  final val TASK_PARAM_START_DATE: String = "startDate"
  final val TASK_PARAM_END_DATE: String = "endDate"

  /**
    * session相关常量
    */
  val SESSION_ID = "sid"
  val SESSION_COUNTRY = "country"
  val SESSION_PROVINCE = "province"
  val SESSION_CITY = "city"
  val SESSION_VISIT_TIME_LENGTH = "visitTimeLength"
  val SESSION_VISIT_STEP_LENGTH = "visitStepLegth"
  val SESSION_KEYWORDS = "keywords"
  val SESSION_GOODS_ID = "gid"


  /**
    * 累加器相关常量
    */
  final val SESSION_COUNT = "session_count"
  final val TIME_1s_3s = "1s_3s"
  final val TIME_4s_6s = "4s_6s"
  final val TIME_7s_9s = "7s_9s"
  final val TIME_10s_30s = "10s_30s"
  final val TIME_30s_60s = "30s_60s"
  final val TIME_1m_3m = "1m_3m"
  final val TIME_3m_10m = "3m_10m"
  final val TIME_10m_30m = "10m_30m"
  final val TIME_30m = "30m"
  final val STEP_1_3 = "1_3"
  final val STEP_4_6 = "4_6"
  final val STEP_7_9 = "7_9"
  final val STEP_10_30 = "10_30"
  final val STEP_30_60 = "30_60"
  final val STEP_60 = "60"


  /**
    * 品类Top5相关的常量
    */
  final val CLICK_COUNT = "click_count"
  final val CART_COUNT = "cart_count"
  final val ORDER_COUNT = "order_count"
  final val PAY_COUNT = "pay_count"


  /**
    * Sparkstreaming 相关常量
    */
  final val GROUP_ID: String = "group.id"
  final val METADATA_BROKER_LIST: String = "metadata.broker.list"
  final val KAFKA_TOPIC: String = "kafka.topic"
  final val AUTO_OFFSET_RESET: String = "auto.offset.reset"
  final val STREAMING_CHECKPOINT_PATH: String = "streaming.checkpoint.path"
  final val BATCH_INTERVAL: String = "batch.interval"



}
