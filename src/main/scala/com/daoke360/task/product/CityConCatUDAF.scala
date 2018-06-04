package com.daoke360.task.product

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

class CityConCatUDAF extends UserDefinedAggregateFunction {
  /**
    * 输入的数据类型
    *
    * @return
    */
  override def inputSchema: StructType = {
    StructType(List(StructField("city", StringType, true)))
  }

  /**
    * 中间结果的数据类型
    *
    * @return
    */
  override def bufferSchema: StructType = {
    StructType(List(StructField("cities", StringType, true)))
  }

  /**
    * 最终结果的数据类型
    *
    * @return
    */
  override def dataType: DataType = {
    StringType
  }

  /**
    * 中间结果类型和最终结果数据类型是否一致,一般用true
    *
    * @return
    */
  override def deterministic: Boolean = {
    true
  }

  /**
    * 每个分组聚合时候的初始值
    *
    * @param buffer
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //buffer(0)=""
    buffer.update(0, "")
  }

  /**
    * 定义每次如何进行聚合操作
    *
    * @param buffer 初始值或聚合后的值 "",晋城,运城
    * @param input  下一个需要聚合的值   太原
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //取出原来的结果
    var value = buffer(0).toString
    //取出下一个需要聚合的城市
    val nextValue = input(0).toString
    value = value + "," + nextValue
    if (value.startsWith(","))
      value.substring(1)
    buffer.update(0, value)
  }

  /**
    * 因为spark是分布式计算，所以需要将多个分区的结果进行合并
    * @param buffer1  初始值或合并后的值
    * @param buffer2  下一个需要合并的分区
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //取出原来的结果
    var value = buffer1(0).toString
    //取出下一个需要聚合的城市
    val nextValue = buffer2(0).toString
    value = value + "," + nextValue
    if (value.startsWith(","))
      value.substring(1)
    buffer1.update(0, value)
  }

  /**
    * 返回最终的结果
    * @param buffer
    * @return
    */
  override def evaluate(buffer: Row): Any = {
    buffer(0)
  }
}
