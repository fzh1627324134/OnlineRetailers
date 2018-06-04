package com.daoke360.task.session

import com.daoke360.common.GlobalContants
import com.daoke360.utils.Utils
import org.apache.spark.{AccumulatorParam, SparkException}


object SessionAggrStatAccumulator extends AccumulatorParam[String] {


  /**
    * 定义累加器的初始值
    *
    * @param initialValue
    * @return session_count=0|1s_3s=0|4s_6s=0|7s_9s=0|10s_30s=0|30s_60s=0|1m_3m=0|3m_10m=0|10m_30m=0|30m=0|1_3=0|4_6=0|7_9=0|10_30=0|30_60=0|60=0
    */
  override def zero(initialValue: String): String = {
    GlobalContants.SESSION_COUNT + "=" + 0 + "|" +
      GlobalContants.TIME_1s_3s + "=" + 0 + "|" +
      GlobalContants.TIME_4s_6s + "=" + 0 + "|" +
      GlobalContants.TIME_7s_9s + "=" + 0 + "|" +
      GlobalContants.TIME_10s_30s + "=" + 0 + "|" +
      GlobalContants.TIME_30s_60s + "=" + 0 + "|" +
      GlobalContants.TIME_1m_3m + "=" + 0 + "|" +
      GlobalContants.TIME_3m_10m + "=" + 0 + "|" +
      GlobalContants.TIME_10m_30m + "=" + 0 + "|" +
      GlobalContants.TIME_30m + "=" + 0 + "|" +
      GlobalContants.STEP_1_3 + "=" + 0 + "|" +
      GlobalContants.STEP_4_6 + "=" + 0 + "|" +
      GlobalContants.STEP_7_9 + "=" + 0 + "|" +
      GlobalContants.STEP_10_30 + "=" + 0 + "|" +
      GlobalContants.STEP_30_60 + "=" + 0 + "|" +
      GlobalContants.STEP_60 + "=" + 0
  }

  /**
    * 定义每次累加器如何进行累加
    *
    * @param r1 初始值或累加后的结果 session_count=0|1s_3s=0|4s_6s=0|7s_9s=0|10s_30s=0|30s_60s=0|1m_3m=0|3m_10m=0|10m_30m=0|30m=0|1_3=0|4_6=0|7_9=0|10_30=0|30_60=0|60=0
    * @param r2 需要累加的字段  session_count
    * @return
    */
  override def addInPlace(r1: String, r2: String): String = {
    if (r1 == "") {
      r2
    } else {
      //0
      var fieldValue = Utils.getFieldValue(r1, r2, "[|]")
      if (fieldValue != null) {
        //1
        fieldValue = (fieldValue.toInt + 1).toString
        //session_count=1|1s_3s=0|4s_6s=0|7s_9s=0|10s_30s=0|30s_60s=0|1m_3m=0|3m_10m=0|10m_30m=0|30m=0|1_3=0|4_6=0|7_9=0|10_30=0|30_60=0|60=0
        fieldValue = Utils.setFieldValue(r1, r2, fieldValue, "[|]")
        fieldValue
      } else {
        var session_count = Utils.getFieldValue(r1, GlobalContants.SESSION_COUNT, "[|]").toInt + Utils.getFieldValue(r2, GlobalContants.SESSION_COUNT, "[|]").toInt
        var time_1s_3s = Utils.getFieldValue(r1, GlobalContants.TIME_1s_3s, "[|]").toInt + Utils.getFieldValue(r2, GlobalContants.TIME_1s_3s, "[|]").toInt
        var time_4s_6s = Utils.getFieldValue(r1, GlobalContants.TIME_4s_6s, "[|]").toInt + Utils.getFieldValue(r2, GlobalContants.TIME_4s_6s, "[|]").toInt
        var time_7s_9s = Utils.getFieldValue(r1, GlobalContants.TIME_7s_9s, "[|]").toInt + Utils.getFieldValue(r2, GlobalContants.TIME_7s_9s, "[|]").toInt
        var time_10s_30s = Utils.getFieldValue(r1, GlobalContants.TIME_10s_30s, "[|]").toInt + Utils.getFieldValue(r2, GlobalContants.TIME_10s_30s, "[|]").toInt
        var time_30s_60s = Utils.getFieldValue(r1, GlobalContants.TIME_30s_60s, "[|]").toInt + Utils.getFieldValue(r2, GlobalContants.TIME_30s_60s, "[|]").toInt
        var time_1m_3m = Utils.getFieldValue(r1, GlobalContants.TIME_1m_3m, "[|]").toInt + Utils.getFieldValue(r2, GlobalContants.TIME_1m_3m, "[|]").toInt
        var time_3m_10m = Utils.getFieldValue(r1, GlobalContants.TIME_3m_10m, "[|]").toInt + Utils.getFieldValue(r2, GlobalContants.TIME_3m_10m, "[|]").toInt
        var time_10m_30m = Utils.getFieldValue(r1, GlobalContants.TIME_10m_30m, "[|]").toInt + Utils.getFieldValue(r2, GlobalContants.TIME_10m_30m, "[|]").toInt
        var time_30m = Utils.getFieldValue(r1, GlobalContants.TIME_30m, "[|]").toInt + Utils.getFieldValue(r2, GlobalContants.TIME_30m, "[|]").toInt
        var step_1_3 = Utils.getFieldValue(r1, GlobalContants.STEP_1_3, "[|]").toInt + Utils.getFieldValue(r2, GlobalContants.STEP_1_3, "[|]").toInt
        var step_4_6 = Utils.getFieldValue(r1, GlobalContants.STEP_4_6, "[|]").toInt + Utils.getFieldValue(r2, GlobalContants.STEP_4_6, "[|]").toInt
        var step_7_9 = Utils.getFieldValue(r1, GlobalContants.STEP_7_9, "[|]").toInt + Utils.getFieldValue(r2, GlobalContants.STEP_7_9, "[|]").toInt
        var step_10_30 = Utils.getFieldValue(r1, GlobalContants.STEP_10_30, "[|]").toInt + Utils.getFieldValue(r2, GlobalContants.STEP_10_30, "[|]").toInt
        var step_30_60 = Utils.getFieldValue(r1, GlobalContants.STEP_30_60, "[|]").toInt + Utils.getFieldValue(r2, GlobalContants.STEP_30_60, "[|]").toInt
        var step_60 = Utils.getFieldValue(r1, GlobalContants.STEP_60, "[|]").toInt + Utils.getFieldValue(r2, GlobalContants.STEP_60, "[|]").toInt

        GlobalContants.SESSION_COUNT + "=" + session_count + "|" +
          GlobalContants.TIME_1s_3s + "=" + time_1s_3s + "|" +
          GlobalContants.TIME_4s_6s + "=" + time_4s_6s + "|" +
          GlobalContants.TIME_7s_9s + "=" + time_7s_9s + "|" +
          GlobalContants.TIME_10s_30s + "=" + time_10s_30s + "|" +
          GlobalContants.TIME_30s_60s + "=" + time_30s_60s + "|" +
          GlobalContants.TIME_1m_3m + "=" + time_1m_3m + "|" +
          GlobalContants.TIME_3m_10m + "=" + time_3m_10m + "|" +
          GlobalContants.TIME_10m_30m + "=" + time_10m_30m + "|" +
          GlobalContants.TIME_30m + "=" + time_30m + "|" +
          GlobalContants.STEP_1_3 + "=" + step_1_3 + "|" +
          GlobalContants.STEP_4_6 + "=" + step_4_6 + "|" +
          GlobalContants.STEP_7_9 + "=" + step_7_9 + "|" +
          GlobalContants.STEP_10_30 + "=" + step_10_30 + "|" +
          GlobalContants.STEP_30_60 + "=" + step_30_60 + "|" +
          GlobalContants.STEP_60 + "=" + step_60
      }
    }
  }
}

