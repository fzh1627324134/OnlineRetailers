package com.daoke360.dao

import java.sql.ResultSet

import com.daoke360.jdbc.JdbcManager
import com.daoke360.domain.SparkTask

object SparkTaskDao {

  /**
    * 通过任务id返回任务对应信息，封装到SparkTask实体类中
    */
  def getSparkTaskById(task_id: Int) = {
    var sparkTask: SparkTask = null
    val sql = "select task_id,task_param from spark_task where task_id=?"
    val params = Array[Any](task_id)
    JdbcManager.executeQuery(sql, params, (resultSet: ResultSet) => {
      if (resultSet.next()) {
        sparkTask = new SparkTask(
          resultSet.getInt("task_id"),
          resultSet.getString("task_param")
        )
      }
    })
    sparkTask
  }



}
