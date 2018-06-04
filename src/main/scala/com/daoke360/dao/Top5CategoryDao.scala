package com.daoke360.dao

import com.daoke360.domain.Top5Category
import com.daoke360.jdbc.JdbcManager

object Top5CategoryDao {

  def deleteByTaskId(task_id: Int) = {
    val sql = "delete from top5_category where task_id=?"
    val params = Array[Any](task_id)
    JdbcManager.excuteUpdate(sql, params)
  }

  def insertEntities(top5CategoryArray: Array[Top5Category]) = {
    val sql = "insert into top5_category values(?,?,?,?,?,?)"
    val paramsArray = new Array[Array[Any]](top5CategoryArray.length)
    for (i <- 0 until (top5CategoryArray.length)) {
      val top5Category = top5CategoryArray(i)
      paramsArray(i)=Array[Any](
        top5Category.task_id,
        top5Category.click_count,
        top5Category.cart_count,
        top5Category.order_count,
        top5Category.pay_count
      )
    }
    JdbcManager.executeBatch(sql,paramsArray)
  }

}
