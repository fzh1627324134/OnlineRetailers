package com.daoke360.utils

import java.text.SimpleDateFormat
import java.util.regex.Pattern
import java.math.BigDecimal
import java.util.Calendar
import com.daoke360.enum.DateEnum
import scala.math.BigDecimal.RoundingMode

object Utils {


  /**
    * 获取时间戳 对应的各个时间单位
    *
    * @param longTime 时间戳
    * @param dateType 时间单位： 年，季度，月，周，天
    * @return
    */
  def getDateInfo(longTime: Long, dateType: DateEnum.Value) = {
    val calendar = Calendar.getInstance()
    calendar.setTimeInMillis(longTime)
    if (dateType.equals(DateEnum.year)) {
      calendar.get(Calendar.YEAR)
    } else if (dateType.equals(DateEnum.season)) {
      val month = calendar.get(Calendar.MONTH) + 1
      if (month % 3 == 0) {
        month / 3
      } else {
        month / 3 + 1
      }
    } else if (dateType.equals(DateEnum.month)) {
      calendar.get(Calendar.MONTH) + 1
    } else if (dateType.equals(DateEnum.week)) {
      calendar.get(Calendar.WEEK_OF_YEAR)
    } else if (dateType.equals(DateEnum.day)) {
      calendar.get(Calendar.DAY_OF_MONTH)
    } else {
      throw new Exception("获取时间单位信息失败...")
    }
  }

  /**
    * 四舍五入函数
    *
    * @param doubleValue 需要进行四舍五入的值
    * @param scala       保留的小数位
    * @return
    */
  def getScale(doubleValue: Double, scala: Int) = {
    val bigDecimal = new BigDecimal(doubleValue)
    bigDecimal.setScale(scala, RoundingMode.UP).doubleValue()
  }

  /**
    * 设置字符串指定字段的值，并返回新的结果字符串
    *
    * @param value
    * @param fieldName  1s_2s
    * @param fieldValue 12
    * @param separator
    * @return
    */
  def setFieldValue(value: String, fieldName: String, fieldValue: String, separator: String): String = {
    //Array(session_count=6,1s_3s=0,4s_6s=0,...)
    val items = value.split(separator)
    for (i <- 0 until (items.length)) {
      var item = items(i)
      val Array(key, value) = item.split("=")
      if (key.equals(fieldName)) {
        item = fieldName + "=" + fieldValue //1s_2s=12
        items(i) = item
      }
    }
    items.mkString("|")
  }

  /**
    * 获取字符串中指定的字段的值
    *
    * @param value     session_count=0|1s_3s=0|4s_6s=0|7s_9s=0|10s_30s=0|30s_60s=0|
    *                  1m_3m=0|3m_10m=0|10m_30m=0|30m=0|1_3=0|4_6=0|7_9=0|10_30=0|30_60=0|60=0
    * @param fieldName session_count
    * @param separator |
    * @return 0
    */
  def getFieldValue(value: String, fieldName: String, separator: String): String = {
    //Array(session_count=6,1s_3s=0,4s_6s=0,...)
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
    * 将时间戳转换城指定格式的日期
    */
  def formatDate(longTime: Long, pattern: String) = {
    val sdf = new SimpleDateFormat(pattern)
    val calendar = sdf.getCalendar
    calendar.setTimeInMillis(longTime)
    sdf.format(calendar.getTime)
  }

  /**
    * 指定格式的日期转换成long类型的时间戳
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
    * 将nginx服务器时间转换为long类型的时间
    */
  def nginxTime2Long(nginxTime: String): Long = {
    (nginxTime.toDouble * 1000).toLong
  }

}
