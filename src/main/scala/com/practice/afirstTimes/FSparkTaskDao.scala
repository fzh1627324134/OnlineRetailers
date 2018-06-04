package com.practice.afirstTimes

import java.sql.ResultSet

object FSparkTaskDao {

  /**
    * 通过任务id返回任务对应信息，封装到SparkTask实体类中
    */
  def getSparkTaskById(task_id: Int) = {
    var sparkTask: FSparkTask = null
    val sql = "select task_id,task_param from event_log where task_id=?"
    val params = Array[Any](task_id)
    FJdbcManager.executeQuery(sql, params, (resultSet: ResultSet) => {
      if (resultSet.next()) {
        sparkTask = new FSparkTask(
          resultSet.getInt("task_id"),
          resultSet.getString("task_param")
        )
      }
    })
    sparkTask
  }
}
