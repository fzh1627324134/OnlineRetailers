package com.practice.afirstTimes

import java.math.BigDecimal
import java.text.SimpleDateFormat
import java.util.regex.Pattern

import scala.math.BigDecimal.RoundingMode

object FUtils {

  def getScala(doubleValue: Double, scala: Int) = {
    val bigDecimal = new BigDecimal(doubleValue)
    bigDecimal.setScale(scala, RoundingMode.UP).doubleValue()
  }

  def setFieldValue(value: String, fieldName: String, fieldValue: String, separator: String): String = {
    val items = value.split(separator)
    for (i <- 0 until (items.length)) {
      var item = items(i)
      val Array(key, value) = item.split("=")
      if (key.equals(fieldName)) {
        item = fieldName + "=" + fieldValue
        items(i) = item
      }
    }
    items.mkString("|")
  }

  /**
    * 获取字符串中指定字段的值
    *
    * @param value     session_count=6|1s_3s=0|4s_6s=0|7s_9s=0|10s_30s=0|30s_60s=0|1m_3m=0|3m_10m=0|10m_30m=0|30m=0|1_3=0|4_6=0|7_9=0|10_30=0|30_60=0|60=0
    * @param fieldName session_count
    * @param separator |
    */
  def getFieldValue(value: String, fieldName: String, separator: String): String = {
    val items = value.split(separator)
    for (item <- items) {
      val Array(key, value) = item.split("=")
      if (key.equals(fieldName)) {
        return value
      }
    }
    null
  }

  def validateNumber(inputNumber: String) = {
    val reg = "\\d+"
    val pattern = Pattern.compile(reg)
    pattern.matcher(inputNumber).matches()
  }

  /**
    * 将时间戳转换成指定格式的时间
    */
  def formtDate(longTime: Long, pattern: String) = {
    val sdf = new SimpleDateFormat(pattern)
    val calendar = sdf.getCalendar
    calendar.setTimeInMillis(longTime)
    sdf.format(calendar.getTime)
  }

  /**
    * 将指定格式的日期转换成long类型的时间戳
    */
  def parseDate2Long(inputDate: String, pattern: String) = {
    val sdf = new SimpleDateFormat(pattern)
    val date = sdf.parse(inputDate)
    date.getTime
  }

  /**
    * 验证日期是否是yyyy-MM-dd这种格式
    */
  def validateInpuDate(inputDate: String) = {
    val reg = "^((((1[6-9]|[2-9]\\d)\\d{2})-(0?[13578]|1[02])-(0?[1-9]|[12]\\d|3[01]))|(((1[6-9]|[2-9]\\d)\\d{2})-(0?[13456789]|1[012])-(0?[1-9]|[12]\\d|30))|(((1[6-9]|[2-9]\\d)\\d{2})-0?2-(0?[1-9]|1\\d|2[0-8]))|(((1[6-9]|[2-9]\\d)(0[48]|[2468][048]|[13579][26])|((16|[2468][048]|[3579][26])00))-0?2-29-))$"
    val pattern = Pattern.compile(reg)
    pattern.matcher(inputDate).matches()
  }

  /**
    * 将nginx服务器时间转换成long类型的时间
    */
  def nginxTime2Long(nginxTime: String): Long = {
    (nginxTime.toDouble * 1000).toLong
  }

}
