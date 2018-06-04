package com.daoke360.dao

import com.daoke360.domain.StatsLocationFlow
import com.daoke360.jdbc.JdbcManager

object StatsLocationFlowDao {

  def update(statsLocationFlowArray: Array[StatsLocationFlow]) = {
    val sql =
      """
        |insert into stats_location_flow(date_dimension_id,location_dimension_id,nu,uv,pv,sn,`on`,created)values(?,?,?,?,?,?,?,?)
        |on duplicate key update nu=?,uv=?,pv=?,sn=?,`on`=?
      """.stripMargin

    val paramsArray = new Array[Array[Any]](statsLocationFlowArray.length)
    for (i <- 0 until (statsLocationFlowArray.length)) {
      val statsLocationFlow = statsLocationFlowArray(i)
      paramsArray(i) = Array[Any](
        statsLocationFlow.date_dimension_id,
        statsLocationFlow.location_dimension_id,
        statsLocationFlow.nu,
        statsLocationFlow.uv,
        statsLocationFlow.pv,
        statsLocationFlow.sn,
        statsLocationFlow.on,
        statsLocationFlow.created,
        statsLocationFlow.nu,
        statsLocationFlow.uv,
        statsLocationFlow.pv,
        statsLocationFlow.sn,
        statsLocationFlow.on
      )
    }
    JdbcManager.executeBatch(sql, paramsArray)
  }


}
