package com.daoke360.domain

class SparkTask(var task_id: Int, var task_param: String) {

  override def toString: String = s"SparkTask($task_id,$task_param)"

}
