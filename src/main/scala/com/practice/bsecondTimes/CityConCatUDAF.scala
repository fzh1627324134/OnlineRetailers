package com.practice.bsecondTimes

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

class CityConCatUDAF extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = {
    StructType(List(StructField("city", StringType, true)))
  }

  override def bufferSchema: StructType = {
    StructType(List(StructField("cities",StringType,true)))
  }

  override def dataType: DataType = {
    StringType
  }

  override def deterministic: Boolean = {
    true
  }

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0,"")
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    var value = buffer(0).toString
    val nextValue = input(0).toString
    value = value + "," + nextValue
    if (value.startsWith(","))
      value = value.substring(1)
    buffer.update(0,value)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var value = buffer1(0).toString
    val nextValue = buffer2(0).toString
    value = value + "," +nextValue
    if (value.startsWith(","))
      value.substring(1)
    buffer1.update(0,value)
  }

  override def evaluate(buffer: Row): Any = {
    buffer(0)
  }
}
