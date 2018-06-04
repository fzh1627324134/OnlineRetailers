package com.daoke360.task.session

import java.sql.ResultSet

import com.daoke360.jdbc.JdbcManager
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object TestDemo {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val cidRDD = new JdbcRDD[(Long, Long)](
      sc,
      () => {
        val connection = JdbcManager.getConnection()
        println(connection)
        connection
      },
      "select goods_id,cat_id from ecs_goods where goods_id>? and goods_id<?",
      0,
      Long.MaxValue,
      2,
      (resultSet: ResultSet) => {
        (resultSet.getLong("goods_id"), resultSet.getLong("cat_id"))
      }
    )
    cidRDD.collect().foreach(println(_))
    sc.stop()
  }

}
