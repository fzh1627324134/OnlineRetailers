package com.daoke360.dao

import com.daoke360.domain.AreaTop3Product
import com.daoke360.jdbc.JdbcManager

object AreaTop3ProductDao {

  def deleteByTaskId(task_id: Int) = {
    val sql = "delete from area_top3_product where task_id=?"
    val params = Array[Any](task_id)
    JdbcManager.excuteUpdate(sql, params)
  }

  def insertEntities(areaTop3ProductArray: Array[AreaTop3Product]) = {
    val sql = "insert into area_top3_product values(?,?,?,?,?,?)"
    val paramsArray = new Array[Array[Any]](areaTop3ProductArray.length)
    for (i<-0 until(areaTop3ProductArray.length)){
      val areaTop3Product = areaTop3ProductArray(i)
      paramsArray(i) = Array[Any](
        areaTop3Product.task_id,
        areaTop3Product.province,
        areaTop3Product.product_id,
        areaTop3Product.product_name,
        areaTop3Product.click_count,
        areaTop3Product.city_infos
      )
    }
    JdbcManager.executeBatch(sql,paramsArray)
  }

}
