package com.practice.bsecondTimes

import com.alibaba.fastjson.{JSON, JSONObject}
import com.practice.afirstTimes._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, SparkException}

import scala.collection.mutable.ArrayBuffer

object BSessionAggrStatTask {

  def setVlaue(value: String, fieldName: String, fieldValue: String, separator: String): String = {
    val items = value.split(separator)
    for (i <- 0 until(items.length)){
      var item = items(i)
      val Array(key,value) = item.split("=")
      if (key.equals(fieldName)){
        item = fieldName+"="+fieldValue
        items(i) = item
      }
    }
    items.mkString("|")
  }

  def getValue(value: String, filedName: String, separator: String): String = {
    val items = value.split(separator)
    for (item <- items) {
      val Array(key, value) = item.split("=")
      if (key.equals(filedName)) {
        return value
      }
    }
    null
  }

  def processArgs(sparkConf: SparkConf, args: Array[String]) = {
    if (args.length == 2 && args(0).equals(FGlobalContants.TASK_PARAMS_FLAG) && FUtils.validateNumber(args(1))) {
      sparkConf.set(FGlobalContants.TASK_RUN_ID, args(1))
    } else {
      throw new SparkException(s"输入的参数错误: -d task_id")
    }
  }

  def initScan(sparkParamObject: JSONObject) = {
    //取出统计时间范围的开始时间和结束时间
    val startDate = sparkParamObject.getString(FGlobalContants.TASK_PARAM_START_DATE)
    val endDate = sparkParamObject.getString(FGlobalContants.TASK_PARAM_END_DATE)
    //将开始时间和结束时间，转换成时间戳
    val startTime = FUtils.parseDate2Long(startDate, "yyyy-MM-dd HH:mm:ss").toString
    val endTime = FUtils.parseDate2Long(endDate, "yyyy-MM-dd HH:mm:ss").toString

    val scan = new Scan()
    //设置扫描器的开始和结束位置（包前不包后）
    scan.setStartRow(startTime.getBytes())
    scan.setStopRow(endTime.getBytes())
    scan
  }

  def sessionFullInfo(ssessionGroupRDD: RDD[(String, Iterable[(String, String, String, Long, String, String, String, String, String)])]) = {
    val sessionFullInfoRDD = ssessionGroupRDD.map(t2 => {
      var country, province, city: String = null
      var startTime, endTime: Long = 0
      var visitTimeLength: Long = 0
      var visitStepLength: Int = 0
      var goodsIdsBuffer = ArrayBuffer[String]()
      var keywordsBuffer = ArrayBuffer[String]()
      t2._2.foreach(t9 => {
        if (country == null || province == null || city == null) {
          country = t9._5
          province = t9._6
          city = t9._7
        }
        if (startTime == 0 || startTime > t9._4) startTime = t9._4
        if (endTime == 0 || endTime < t9._4) endTime = t9._4

        if (t9._2.equals(FEventEnum.pageViewEvent.toString)) {
          visitStepLength += 1
          if (t9._9 != null)
            goodsIdsBuffer += t9._9
        }
        if (t9._2.equals(FEventEnum.searchEvent.toString))
          keywordsBuffer += t9._8
      })
      visitTimeLength = (endTime - startTime) / 1000
      var sessionFullInfo = FGlobalContants.SESSION_ID + "=" + t2._1 + "|"
      sessionFullInfo += FGlobalContants.SESSION_COUNTRY + "=" + country + "|"
      sessionFullInfo += FGlobalContants.SESSION_PROVINCE + "=" + province + "|"
      sessionFullInfo += FGlobalContants.SESSION_CITY + "=" + city + "|"
      sessionFullInfo += FGlobalContants.SESSION_VISIT_TIME_LENGTH + "=" + visitTimeLength + "|"
      sessionFullInfo += FGlobalContants.SESSION_VISIT_STEP_LENGTH + "=" + visitStepLength + "|"

      if (goodsIdsBuffer.size > 2)
        sessionFullInfo += FGlobalContants.SESSION_GOODS_ID + "=" + goodsIdsBuffer + "|"

      if (keywordsBuffer.size > 2)
        sessionFullInfo += FGlobalContants.SESSION_KEYWORDS + "=" + keywordsBuffer + "|"

      sessionFullInfo = sessionFullInfo.substring(0, sessionFullInfo.length - 1)
      (t2._1, sessionFullInfo)
    })
    sessionFullInfoRDD
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
    processArgs(sparkConf, args)
    //去数据库中查找任务对饮的信息
    val sparkTask = FSparkTaskDao.getSparkTaskById(sparkConf.get(FGlobalContants.TASK_RUN_ID).toInt)
    if (sparkTask == null) {
      throw new SparkException(s"从数据库获取任务失败")
    }
    val sparkParamObject = JSON.parseObject(sparkConf.get(sparkTask.task_param).toString)
    val scan: Scan = initScan(sparkParamObject)
    //接下来需要将原始的scan转换成可以进行base64转码的scan
    val protoScan = ProtobufUtil.toScan(scan)
    val base64Scan = Base64.encodeBytes(protoScan.toByteArray)

    val jobConf = new JobConf(new Configuration())
    //设置要读取的表
    jobConf.set(TableInputFormat.INPUT_TABLE, FEventLogContants.HBASE_EVENT_LOG_TABLE)

    //设置扫描器去扫描指定的表
    jobConf.set(TableInputFormat.SCAN, base64Scan)

    val sc = new SparkContext(sparkConf)

    val eventLogRDD = sc.newAPIHadoopRDD(jobConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]).map(_._2).map(result => {
      val uuid = Bytes.toString(result.getValue(FEventLogContants.HBASE_EVENT_LOG_TABLE_FAMILY.getBytes(),
        FEventLogContants.LOG_COLUMN_NAME_UUID.getBytes()))
      val sessionId = Bytes.toString(result.getValue(FEventLogContants.HBASE_EVENT_LOG_TABLE_FAMILY.getBytes(),
        FEventLogContants.LOG_COLUMN_NAME_SID.getBytes()))
      val access_time = Bytes.toString(result.getValue(FEventLogContants.HBASE_EVENT_LOG_TABLE_FAMILY.getBytes(),
        FEventLogContants.LOG_COLUMN_NAME_ACCESS_TIME.getBytes())).toLong
      val eventName = Bytes.toString(result.getValue(FEventLogContants.HBASE_EVENT_LOG_TABLE_FAMILY.getBytes(),
        FEventLogContants.LOG_COLUMN_NAME_EVENT_NAME.getBytes()))
      val country = Bytes.toString(result.getValue(FEventLogContants.HBASE_EVENT_LOG_TABLE_FAMILY.getBytes(),
        FEventLogContants.LOG_COLUMN_NAME_COUNTRY.getBytes()))
      val province = Bytes.toString(result.getValue(FEventLogContants.HBASE_EVENT_LOG_TABLE_FAMILY.getBytes(),
        FEventLogContants.LOG_COLUMN_NAME_PROVINCE.getBytes()))
      val city = Bytes.toString(result.getValue(FEventLogContants.HBASE_EVENT_LOG_TABLE_FAMILY.getBytes(),
        FEventLogContants.LOG_COLUMN_NAME_CITY.getBytes()))
      val keyword = Bytes.toString(result.getValue(FEventLogContants.HBASE_EVENT_LOG_TABLE_FAMILY.getBytes(),
        FEventLogContants.LOG_COLUMN_NAME_KEYWORD.getBytes()))
      val gid = Bytes.toString(result.getValue(FEventLogContants.HBASE_EVENT_LOG_TABLE_FAMILY.getBytes(),
        FEventLogContants.LOG_COLUMN_NAME_GOODS_ID.getBytes()))
      (uuid, sessionId, eventName, access_time, country, province, city, keyword, gid)
    })

    val sessionGroupRDD = eventLogRDD.groupBy(_._2)

    val sessionFullInfoRDD = sessionFullInfo(sessionGroupRDD)


  }

}
