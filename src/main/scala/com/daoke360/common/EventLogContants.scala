package com.daoke360.common

object EventLogContants {

  /**
    * 保存日志的表
    */
  final val HBASE_EVENT_LOG_TABLE: String = "event_log"

  /**
    * 列族
    */
  final val HBASE_EVENT_LOG_TABLE_FAMILY: String = "log"

  /**
    * 用户ip
    */
  final val LOG_COLUMN_NAME_IP: String = "ip"
  /**
    * 用户访问时间
    */
  final val LOG_COLUMN_NAME_ACCESS_TIME = "access_time"
  /**
    * 国家
    */
  final val LOG_COLUMN_NAME_COUNTRY = "country"
  /**
    * 省份
    */
  final val LOG_COLUMN_NAME_PROVINCE = "province"
  /**
    * 城市
    */
  final val LOG_COLUMN_NAME_CITY = "city"

  /**
    * 用户唯一标识
    */
  final val LOG_COLUMN_NAME_UUID = "uid"

  /**
    * 用户会话标识
    */
  final val LOG_COLUMN_NAME_SID = "sid"


  /**
    * 事件名称
    */
  final val LOG_COLUMN_NAME_EVENT_NAME = "en"

  /**
    * 搜索关键词
    */
  final val LOG_COLUMN_NAME_KEYWORD = "kw"

  /**
    * 商品id
    */
  final val LOG_COLUMN_NAME_GOODS_ID = "gid"

  /**
    * 浏览器名称
    */
  final val  LOG_COLUMN_NAME_BROWSER_NAME="b_n"
}
