package com.practice.afirstTimes

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulator, SparkConf, SparkContext, SparkException}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by 毛爷爷我男神i on 2018/4/4.
  */
object FSessionAggrStatTask {

  def processArgs(sparkConf: SparkConf, args: Array[String]) = {
    if (args.length == 2 && args(0).equals(FGlobalContants.TASK_PARAMS_FLAG) && FUtils.validateNumber(args(1))) {
      sparkConf.set(FGlobalContants.TASK_RUN_ID, args(1))
    } else {
      throw new SparkException(s"输入参数错误：-d task_id")
    }
  }

  /**
    * 初始化扫描器
    */
  def initScan(sparkParamObject: JSONObject) = {
    val startDate = sparkParamObject.getString(FGlobalContants.TASK_PARAM_START_DATE)
    val endDate = sparkParamObject.getString(FGlobalContants.TASK_PARAM_END_DATE)
    val startTime = FUtils.parseDate2Long(startDate, "yyyy-MM-dd HH:mm:ss").toString
    val endTime = FUtils.parseDate2Long(endDate, "yyyy-MM-dd HH:mm:ss").toString
    val scan = new Scan()
    scan.setStartRow(startTime.getBytes())
    scan.setStopRow(endTime.getBytes())
    scan
  }

  def sessionFullInfo(sessionGroupRDD: RDD[(String, Iterable[(String, String, String, Long, String, String, String, String, String)])]) = {
    val sessionFullInfoRDD = sessionGroupRDD.map(t2 => {
      var country, province, city: String = null
      //会话开始时间和结束时间
      var startTime, endTime: Long = 0
      var visiTimeLength: Long = 0
      var visitStepLength: Long = 0
      var goodsIdBuffer = ArrayBuffer[String]()
      var keywordsBuffer = ArrayBuffer[String]()

      t2._2.foreach(t9 => {
        if (country == null || province == null || city == null) {
          country = t9._5
          province = t9._6
          city = t9._7
        }
        //访问时间
        val access_time = t9._4
        if (startTime == 0 || access_time < startTime) startTime = access_time
        if (endTime == 0 || access_time > endTime) endTime == access_time

        //事件名称
        val eventName = t9._3
        if (eventName.equals(FEventEnum.pageViewEvent.toString)) {
          visitStepLength += 1
          val gid = t9._9
          if (gid != null)
            goodsIdBuffer += gid
        }
        if (eventName.equals(FEventEnum.searchEvent.toString)) {
          keywordsBuffer += t9._8
        }
      })

      visiTimeLength = (endTime - startTime) / 1000
      var sessionFullInfo = FGlobalContants.SESSION_ID + "=" + t2._1 + "|"
      sessionFullInfo += FGlobalContants.SESSION_COUNTRY + "=" + country + "|"
      sessionFullInfo += FGlobalContants.SESSION_PROVINCE + "=" + province + "|"
      sessionFullInfo += FGlobalContants.SESSION_CITY + "=" + city + "|"
      sessionFullInfo += FGlobalContants.SESSION_VISIT_STEP_LENGTH + "=" + visitStepLength + "|"
      sessionFullInfo += FGlobalContants.SESSION_VISIT_TIME_LENGTH + "=" + visiTimeLength + "|"
      if (goodsIdBuffer.size > 0)
        sessionFullInfo += FGlobalContants.SESSION_GOODS_ID + "=" + goodsIdBuffer.mkString(",") + "|"
      if (keywordsBuffer.size > 0)
        sessionFullInfo += FGlobalContants.SESSION_KEYWORDS + "=" + keywordsBuffer.mkString(",") + "|"
      sessionFullInfo = sessionFullInfo.substring(0, sessionFullInfo.length - 1)
      (t2._1, sessionFullInfo)
    })
    sessionFullInfoRDD
  }

  def caculatorVisitTimeLengthRange(visitTimeLength: Int, sessionAggrStatAccumulator: Accumulator[String]) = {
    if (visitTimeLength >= 0 && visitTimeLength <= 3) {
      sessionAggrStatAccumulator.add(FGlobalContants.TIME_1s_3s)
    } else if (visitTimeLength >= 4 && visitTimeLength <= 6) {
      sessionAggrStatAccumulator.add(FGlobalContants.TIME_4s_6s)
    } else if (visitTimeLength >= 7 && visitTimeLength <= 9) {
      sessionAggrStatAccumulator.add(FGlobalContants.TIME_7s_9s)
    } else if (visitTimeLength >= 10 && visitTimeLength <= 30) {
      sessionAggrStatAccumulator.add(FGlobalContants.TIME_10s_30s)
    } else if (visitTimeLength > 30 && visitTimeLength <= 60) {
      sessionAggrStatAccumulator.add(FGlobalContants.TIME_30s_60s)
    } else if (visitTimeLength > 1 * 60 && visitTimeLength <= 3 * 60) {
      sessionAggrStatAccumulator.add(FGlobalContants.TIME_1m_3m)
    } else if (visitTimeLength > 3 * 60 && visitTimeLength <= 10 * 60) {
      sessionAggrStatAccumulator.add(FGlobalContants.TIME_3m_10m)
    } else if (visitTimeLength > 10 * 60 && visitTimeLength <= 30 * 60) {
      sessionAggrStatAccumulator.add(FGlobalContants.TIME_10m_30m)
    } else if (visitTimeLength > 30 * 60) {
      sessionAggrStatAccumulator.add(FGlobalContants.TIME_30m)
    }
  }

  def caculatorVisitStepLengthRange(visitStepLength: Int, sessionAggrStatAccumulator: Accumulator[String]) = {
    if (visitStepLength >= 1 && visitStepLength <= 3) {
      sessionAggrStatAccumulator.add(FGlobalContants.STEP_1_3)
    } else if (visitStepLength >= 4 && visitStepLength <= 6) {
      sessionAggrStatAccumulator.add(FGlobalContants.STEP_4_6)
    } else if (visitStepLength >= 7 && visitStepLength <= 9) {
      sessionAggrStatAccumulator.add(FGlobalContants.STEP_7_9)
    } else if (visitStepLength >= 10 && visitStepLength <= 30) {
      sessionAggrStatAccumulator.add(FGlobalContants.STEP_10_30)
    } else if (visitStepLength > 30 && visitStepLength <= 60) {
      sessionAggrStatAccumulator.add(FGlobalContants.STEP_30_60)
    } else if (visitStepLength > 60) {
      sessionAggrStatAccumulator.add(FGlobalContants.STEP_60)
    }
  }

  def saveResultToMysql(value: String, task_id: Int) = {
    val sessionAggrStat = new FSessionAggrStat()
    sessionAggrStat.task_id = task_id
    sessionAggrStat.session_count = FUtils.getFieldValue(value, FGlobalContants.SESSION_COUNT, "[|]").toInt
    sessionAggrStat.time_1s_3s = FUtils.getScala(FUtils.getFieldValue(value, FGlobalContants.TIME_1s_3s, "[|]")
      .toDouble / sessionAggrStat.session_count, 2)
    sessionAggrStat.time_4s_6s = FUtils.getScala(FUtils.getFieldValue(value, FGlobalContants.TIME_4s_6s, "[|]")
      .toDouble / sessionAggrStat.session_count, 2)
    sessionAggrStat.time_7s_9s = FUtils.getScala(FUtils.getFieldValue(value, FGlobalContants.TIME_7s_9s, "[|]")
      .toDouble / sessionAggrStat.session_count, 2)
    sessionAggrStat.time_10s_30s = FUtils.getScala(FUtils.getFieldValue(value, FGlobalContants.TIME_10s_30s, "[|]")
      .toDouble / sessionAggrStat.session_count, 2)
    sessionAggrStat.time_30s_60s = FUtils.getScala(FUtils.getFieldValue(value, FGlobalContants.TIME_30s_60s, "[|]")
      .toDouble / sessionAggrStat.session_count, 2)
    sessionAggrStat.time_1m_3m = FUtils.getScala(FUtils.getFieldValue(value, FGlobalContants.TIME_1m_3m, "[|]")
      .toDouble / sessionAggrStat.session_count, 2)
    sessionAggrStat.time_3m_10m = FUtils.getScala(FUtils.getFieldValue(value, FGlobalContants.TIME_3m_10m, "[|]")
      .toDouble / sessionAggrStat.session_count, 2)
    sessionAggrStat.time_10m_30m = FUtils.getScala(FUtils.getFieldValue(value, FGlobalContants.TIME_10m_30m, "[|]")
      .toDouble / sessionAggrStat.session_count, 2)
    sessionAggrStat.time_30m = FUtils.getScala(FUtils.getFieldValue(value, FGlobalContants.TIME_30m, "[|]")
      .toDouble / sessionAggrStat.session_count, 2)
    sessionAggrStat.step_1_3 = FUtils.getScala(FUtils.getFieldValue(value, FGlobalContants.STEP_1_3, "[|]")
      .toDouble / sessionAggrStat.session_count, 2)
    sessionAggrStat.step_4_6 = FUtils.getScala(FUtils.getFieldValue(value, FGlobalContants.STEP_4_6, "[|]")
      .toDouble / sessionAggrStat.session_count, 2)
    sessionAggrStat.step_7_9 = FUtils.getScala(FUtils.getFieldValue(value, FGlobalContants.STEP_7_9, "[|]")
      .toDouble / sessionAggrStat.session_count, 2)
    sessionAggrStat.step_10_30 = FUtils.getScala(FUtils.getFieldValue(value, FGlobalContants.STEP_10_30, "[|]")
      .toDouble / sessionAggrStat.session_count, 2)
    sessionAggrStat.step_30_60 = FUtils.getScala(FUtils.getFieldValue(value, FGlobalContants.STEP_30_60, "[|]")
      .toDouble / sessionAggrStat.session_count, 2)
    sessionAggrStat.step_60 = FUtils.getScala(FUtils.getFieldValue(value, FGlobalContants.STEP_60, "[|]")
      .toDouble / sessionAggrStat.session_count, 2)
    FSessionAggrStatDao.deleteByTaskId(task_id)
    FSessionAggrStatDao.inserEntity(sessionAggrStat)
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
    //验证输入参数是否正确
    processArgs(sparkConf, args)
    //去数据库中查找任务对应的信息
    val sparkTask = FSparkTaskDao.getSparkTaskById(sparkConf.get(FGlobalContants.TASK_RUN_ID).toInt)
    if (sparkTask == null)
      throw new SparkException(s"从数据库获取任务失败")
    //{"startDate":"2018-03-29 00:00:00","endDate":"2018-03-30 00:00:00"}
    val sparkParamObject = JSON.parseObject(sparkTask.task_param)
    //初始化扫描器
    val scan: Scan = initScan(sparkParamObject)
    //接下来需要将原始的scan转换可以进行base64转码的scan
    val protoScan = ProtobufUtil.toScan(scan)
    val base64Scan = Base64.encodeBytes(protoScan.toByteArray)
    val jobConf = new JobConf(new Configuration())
    jobConf.set(TableInputFormat.INPUT_TABLE, FEventLogContants.HBASE_EVENT_LOG_TABLE)
    jobConf.set(TableInputFormat.SCAN, base64Scan)
    val sc = new SparkContext(sparkConf)
    val eventLogRDD = sc.newAPIHadoopRDD(jobConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
      .map(_._2).map(result => {
      //用户标识
      val uuid = Bytes.toString(result.getValue(FEventLogContants.HBASE_EVENT_LOG_TABLE_FAMILY.getBytes()
        , FEventLogContants.LOG_COLUMN_NAME_UUID.getBytes()))
      //会话id
      val sessionId = Bytes.toString(result.getValue(FEventLogContants.HBASE_EVENT_LOG_TABLE_FAMILY.getBytes()
        , FEventLogContants.LOG_COLUMN_NAME_SID.getBytes()))
      //访问时间
      val access_time = Bytes.toString(result.getValue(FEventLogContants.HBASE_EVENT_LOG_TABLE_FAMILY.getBytes()
        , FEventLogContants.LOG_COLUMN_NAME_ACCESS_TIME.getBytes())).toLong
      //事件名称
      val eventName = Bytes.toString(result.getValue(FEventLogContants.HBASE_EVENT_LOG_TABLE_FAMILY.getBytes()
        , FEventLogContants.LOG_COLUMN_NAME_EVENT_NAME.getBytes()))
      //国家
      val country = Bytes.toString(result.getValue(FEventLogContants.HBASE_EVENT_LOG_TABLE_FAMILY.getBytes()
        , FEventLogContants.LOG_COLUMN_NAME_COUNTRY.getBytes()))
      //省份
      val province = Bytes.toString(result.getValue(FEventLogContants.HBASE_EVENT_LOG_TABLE_FAMILY.getBytes()
        , FEventLogContants.LOG_COLUMN_NAME_PROVINCE.getBytes()))
      //城市
      val city = Bytes.toString(result.getValue(FEventLogContants.HBASE_EVENT_LOG_TABLE_FAMILY.getBytes()
        , FEventLogContants.LOG_COLUMN_NAME_CITY.getBytes()))
      //搜索关键词
      val keyword = Bytes.toString(result.getValue(FEventLogContants.HBASE_EVENT_LOG_TABLE_FAMILY.getBytes()
        , FEventLogContants.LOG_COLUMN_NAME_KEYWORD.getBytes()))
      //商品id
      val gid = Bytes.toString(result.getValue(FEventLogContants.HBASE_EVENT_LOG_TABLE_FAMILY.getBytes()
        , FEventLogContants.LOG_COLUMN_NAME_GOODS_ID.getBytes()))
      (uuid, sessionId, eventName, access_time, country, province, city, keyword, gid)
    })
    val sessionGroupRDD = eventLogRDD.groupBy(_._2)
    val sessionFullInfoRDD = sessionFullInfo(sessionGroupRDD)
    val sessionAggrStatAccumulator = sc.accumulator("")(FSessionAggrStatAccumulator)
    sessionFullInfoRDD.foreach(t2 => {
      sessionAggrStatAccumulator.add(FGlobalContants.SESSION_COUNT)
      //取出本次session的访问时长和访问步长
      val visitTimeLength = FUtils.getFieldValue(t2._2, FGlobalContants.SESSION_VISIT_TIME_LENGTH, "[|]").toInt
      val visitStepLength = FUtils.getFieldValue(t2._2, FGlobalContants.SESSION_VISIT_STEP_LENGTH, "[|]").toInt
      //计算访问你时长和访问步长所属区间范围
      caculatorVisitTimeLengthRange(visitTimeLength, sessionAggrStatAccumulator)
      caculatorVisitStepLengthRange(visitStepLength, sessionAggrStatAccumulator)
    })
    saveResultToMysql(sessionAggrStatAccumulator.value, sparkTask.task_id)
    sc.stop()
  }

}
