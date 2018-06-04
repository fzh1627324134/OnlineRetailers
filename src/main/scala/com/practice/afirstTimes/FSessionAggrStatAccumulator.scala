package com.practice.afirstTimes

import org.apache.spark.{AccumulatorParam, SparkException}

object FSessionAggrStatAccumulator extends AccumulatorParam[String] {
  override def addInPlace(r1: String, r2: String): String = {
    if (r1 == "") {
      r2
    } else {
      var fieldValue = FUtils.getFieldValue(r1, r2, "[|]")
      if (fieldValue != null) {
        fieldValue = (fieldValue.toInt + 1).toString
        fieldValue = FUtils.setFieldValue(r1,r2,fieldValue,"[|]")
      }else{
        throw new SparkException(s"在字符串中找不到传入进来的key:${r2}")
      }
      fieldValue
    }
  }

  override def zero(initialValue: String): String = {
    FGlobalContants.SESSION_COUNT + "=" + 0 + "|" +
      FGlobalContants.TIME_1s_3s + "=" + 0 + "|" +
      FGlobalContants.TIME_4s_6s + "=" + 0 + "|" +
      FGlobalContants.TIME_7s_9s + "=" + 0 + "|" +
      FGlobalContants.TIME_10s_30s + "=" + 0 + "|" +
      FGlobalContants.TIME_30s_60s + "=" + 0 + "|" +
      FGlobalContants.TIME_1m_3m + "=" + 0 + "|" +
      FGlobalContants.TIME_3m_10m + "=" + 0 + "|" +
      FGlobalContants.TIME_10m_30m + "=" + 0 + "|" +
      FGlobalContants.TIME_30m + "=" + 0 + "|" +
      FGlobalContants.STEP_1_3 + "=" + 0 + "|" +
      FGlobalContants.STEP_4_6 + "=" + 0 + "|" +
      FGlobalContants.STEP_7_9 + "=" + 0 + "|" +
      FGlobalContants.STEP_10_30 + "=" + 0 + "|" +
      FGlobalContants.STEP_30_60 + "=" + 0 + "|" +
      FGlobalContants.STEP_60 + "=" + 0
  }
}
