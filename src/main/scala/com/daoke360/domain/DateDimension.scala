package com.daoke360.domain

import com.daoke360.enum.DateEnum
import com.daoke360.utils.Utils

/**
  * 时间维度实体对象
  */
class DateDimension(var year: Int, var season: Int, var month: Int, var week: Int, var day: Int,
                    var calendar: String, var dateType: String) {

  override def toString = s"DateDimension($year, $season, $month, $week, $day, $calendar, $dateType)"
}

object DateDimension {

  def buildDateDimension(longTime: Long) = {

    val year = Utils.getDateInfo(longTime, DateEnum.year)
    val season = Utils.getDateInfo(longTime, DateEnum.season)
    val month = Utils.getDateInfo(longTime, DateEnum.month)
    var week = Utils.getDateInfo(longTime, DateEnum.week)
    if (week == 1 && month == 12) {
      week = 53
    }
    val day = Utils.getDateInfo(longTime, DateEnum.day)

    new DateDimension(year, season, month, week, day, Utils.formatDate(longTime, "yyyy-MM-dd"), "day")


  }

}
