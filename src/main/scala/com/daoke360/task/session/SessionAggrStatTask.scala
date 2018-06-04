package com.daoke360.task.session

import java.sql.ResultSet

import com.alibaba.fastjson.{JSON, JSONObject}
import com.daoke360.common.{EventLogContants, GlobalContants}
import com.daoke360.dao.{SessionAggrStatDao, SparkTaskDao, Top5CategoryDao}
import com.daoke360.domain.{SessionAggrStat, Top5Category}
import com.daoke360.enum.EventEnum
import com.daoke360.jdbc.JdbcManager
import com.daoke360.utils.Utils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{Accumulator, SparkConf, SparkContext, SparkException}

import scala.collection.mutable.ArrayBuffer

/**
  * 统计出符合条件的session中，访问时长在1s~3s、4s~6s、7s~9s、10s~30s、30s~60s、1m~3m、3m~10m、10m~30m、30m
  * 以上各个范围内的session占比；访问步长在1~3、4~6、7~9、10~30、30~60、60以上各个范围内的session占比
  */
object SessionAggrStatTask {

  def processArgs(sparkConf: SparkConf, args: Array[String]) = {
    if (args.length >= 2 && args(0).equals(GlobalContants.TASK_PARAMS_FLAG) && Utils.validateNumber(args(1))) {
      sparkConf.set(GlobalContants.TASK_RUN_ID, args(1))
    } else {
      throw new SparkException("输入的参数有误：-d task_d")
    }
  }

  /**
    * 初始化扫描器
    *
    * access_time+"_"+ (uuid+enventName).hashcode
    *
    * 2018-03-29 00:00:00
    * 2018-03-29 00:00:01+hascode（）
    * 2018-03-29 00:10:01+hascode（）
    * 2018-03-29 23:59:59+hascode（）
    * 2018-03-30 00:00:00
    *
    * @param sparkParamObject {"startDate":"2018-03-29 00:00:00","endDate":"2018-03-30 00:00:00"}
    */
  def initScan(sparkParamObject: JSONObject) = {
    //取出统计时间范围的开始时间和结束时间
    val startDate = sparkParamObject.getString(GlobalContants.TASK_PARAM_START_DATE)
    val endDate = sparkParamObject.getString(GlobalContants.TASK_PARAM_END_DATE)
    //将开始时间和结束时间，转换成时间戳
    val startTime = Utils.parseDate2Long(startDate, "yyyy-MM-dd HH:mm:ss").toString
    val endTime = Utils.parseDate2Long(endDate, "yyyy-MM-dd HH:mm:ss").toString

    val scan = new Scan()
    //设置扫描器的开始和结束位置（包前不包后）
    scan.setStartRow(startTime.getBytes())
    scan.setStopRow(endTime.getBytes())
    scan
  }

  /**
    * （11，List((1, 11, eventName, access_time, country, province, city, keyword, gid),(1, 11, eventName, access_time, country, province, city, keyword, gid)。。)）
    * （21，List((2, 21, eventName, access_time, country, province, city, keyword, gid),(2, 21, eventName, access_time, country, province, city, keyword, gid)。。)）
    *
    * (11,sid=11|country=china|province=bj|city=bj|visitTimeLength=7|visitStepLegth=8|keyword=xm,hw,pg,mz|goodsId=1,2,3,4|...)
    *
    * @param sessionGroupRDD
    */
  def sessionFullInfo(sessionGroupRDD: RDD[(String, Iterable[(String, String, String, Long, String, String, String, String, String)])]) = {

    val sessionFullInfoRDD = sessionGroupRDD.map(t2 => {
      var country, province, city: String = null
      //会话的开始时间和结束时间
      var startTime, endTime: Long = 0

      var visitTimeLenth: Long = 0
      var visitStepLength: Int = 0
      var goodsIdsBuffer = ArrayBuffer[String]()
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
        if (endTime == 0 || access_time > endTime) endTime = access_time

        //事件名称
        val eventName = t9._3
        if (eventName.equals(EventEnum.pageViewEvent.toString)) {
          visitStepLength += 1
          val gid = t9._9
          if (gid != null)
            goodsIdsBuffer += gid
        }
        if (eventName.equals(EventEnum.searchEvent.toString)) {
          keywordsBuffer += t9._8
        }

      })
      visitTimeLenth = (endTime - startTime) / 1000
      var sessionFullInfo = GlobalContants.SESSION_ID + "=" + t2._1 + "|"
      sessionFullInfo += GlobalContants.SESSION_COUNTRY + "=" + country + "|"
      sessionFullInfo += GlobalContants.SESSION_PROVINCE + "=" + province + "|"
      sessionFullInfo += GlobalContants.SESSION_CITY + "=" + city + "|"
      sessionFullInfo += GlobalContants.SESSION_VISIT_STEP_LENGTH + "=" + visitStepLength + "|"
      sessionFullInfo += GlobalContants.SESSION_VISIT_TIME_LENGTH + "=" + visitTimeLenth + "|"
      if (goodsIdsBuffer.size > 0) {
        sessionFullInfo += GlobalContants.SESSION_GOODS_ID + "=" + goodsIdsBuffer.mkString(",") + "|"
      }
      if (keywordsBuffer.size > 0) {
        sessionFullInfo += GlobalContants.SESSION_KEYWORDS + "=" + keywordsBuffer.mkString(",") + "|"
      }
      sessionFullInfo = sessionFullInfo.substring(0, sessionFullInfo.length - 1)
      (t2._1, sessionFullInfo)
    })
    sessionFullInfoRDD
  }

  /**
    * 计算访问时长所属区间，然后累加器对应的字段+1
    *
    * @param visitTimeLength
    * @param sessionAggrStatAccumulator
    * @return
    */
  def caculatorVisisTimeLengthRange(visitTimeLength: Int, sessionAggrStatAccumulator: Accumulator[String]) = {
    if (visitTimeLength >= 0 && visitTimeLength <= 3) {
      sessionAggrStatAccumulator.add(GlobalContants.TIME_1s_3s)
    } else if (visitTimeLength >= 4 && visitTimeLength <= 6) {
      sessionAggrStatAccumulator.add(GlobalContants.TIME_4s_6s)
    } else if (visitTimeLength >= 7 && visitTimeLength <= 9) {
      sessionAggrStatAccumulator.add(GlobalContants.TIME_7s_9s)
    } else if (visitTimeLength >= 10 && visitTimeLength <= 30) {
      sessionAggrStatAccumulator.add(GlobalContants.TIME_10s_30s)
    } else if (visitTimeLength > 30 && visitTimeLength <= 60) {
      sessionAggrStatAccumulator.add(GlobalContants.TIME_30s_60s)
    } else if (visitTimeLength > 60 * 1 && visitTimeLength <= 60 * 3) {
      sessionAggrStatAccumulator.add(GlobalContants.TIME_1m_3m)
    } else if (visitTimeLength > 60 * 3 && visitTimeLength <= 60 * 10) {
      sessionAggrStatAccumulator.add(GlobalContants.TIME_3m_10m)
    } else if (visitTimeLength > 60 * 10 && visitTimeLength <= 60 * 30) {
      sessionAggrStatAccumulator.add(GlobalContants.TIME_10m_30m)
    } else if (visitTimeLength > 60 * 30) {
      sessionAggrStatAccumulator.add(GlobalContants.TIME_30m)
    }
  }

  /**
    * 计算访问步长所属区间，然后累加器对应的字段+1
    *
    * @param visitStepLength
    * @param sessionAggrStatAccumulator
    * @return
    */
  def caculatorVisitStepLengthRange(visitStepLength: Int, sessionAggrStatAccumulator: Accumulator[String]) = {
    if (visitStepLength >= 1 && visitStepLength <= 3) {
      sessionAggrStatAccumulator.add(GlobalContants.STEP_1_3)
    } else if (visitStepLength >= 4 && visitStepLength <= 6) {
      sessionAggrStatAccumulator.add(GlobalContants.STEP_4_6)
    } else if (visitStepLength >= 7 && visitStepLength <= 9) {
      sessionAggrStatAccumulator.add(GlobalContants.STEP_7_9)
    } else if (visitStepLength >= 10 && visitStepLength <= 30) {
      sessionAggrStatAccumulator.add(GlobalContants.STEP_10_30)
    } else if (visitStepLength > 30 && visitStepLength <= 60) {
      sessionAggrStatAccumulator.add(GlobalContants.STEP_30_60)
    } else if (visitStepLength > 60) {
      sessionAggrStatAccumulator.add(GlobalContants.STEP_60)
    }
  }

  def saveResultToMysql(value: String, task_id: Int) = {
    val sessionAggrStat = new SessionAggrStat
    sessionAggrStat.task_id = task_id
    sessionAggrStat.session_count = Utils.getFieldValue(value, GlobalContants.SESSION_COUNT, "[|]").toInt
    sessionAggrStat.time_1s_3s = Utils.getScale(Utils.getFieldValue(value, GlobalContants.TIME_1s_3s, "[|]").toDouble / sessionAggrStat.session_count, 2)
    sessionAggrStat.time_4s_6s = Utils.getScale(Utils.getFieldValue(value, GlobalContants.TIME_4s_6s, "[|]").toDouble / sessionAggrStat.session_count, 2)
    sessionAggrStat.time_7s_9s = Utils.getScale(Utils.getFieldValue(value, GlobalContants.TIME_7s_9s, "[|]").toDouble / sessionAggrStat.session_count, 2)
    sessionAggrStat.time_10s_30s = Utils.getScale(Utils.getFieldValue(value, GlobalContants.TIME_10s_30s, "[|]").toDouble / sessionAggrStat.session_count, 2)
    sessionAggrStat.time_30s_60s = Utils.getScale(Utils.getFieldValue(value, GlobalContants.TIME_30s_60s, "[|]").toDouble / sessionAggrStat.session_count, 2)
    sessionAggrStat.time_1m_3m = Utils.getScale(Utils.getFieldValue(value, GlobalContants.TIME_1m_3m, "[|]").toDouble / sessionAggrStat.session_count, 2)
    sessionAggrStat.time_3m_10m = Utils.getScale(Utils.getFieldValue(value, GlobalContants.TIME_3m_10m, "[|]").toDouble / sessionAggrStat.session_count, 2)
    sessionAggrStat.time_10m_30m = Utils.getScale(Utils.getFieldValue(value, GlobalContants.TIME_10m_30m, "[|]").toDouble / sessionAggrStat.session_count, 2)
    sessionAggrStat.time_30m = Utils.getScale(Utils.getFieldValue(value, GlobalContants.TIME_30m, "[|]").toDouble / sessionAggrStat.session_count, 2)
    sessionAggrStat.step_1_3 = Utils.getScale(Utils.getFieldValue(value, GlobalContants.STEP_1_3, "[|]").toDouble / sessionAggrStat.session_count, 2)
    sessionAggrStat.step_4_6 = Utils.getScale(Utils.getFieldValue(value, GlobalContants.STEP_4_6, "[|]").toDouble / sessionAggrStat.session_count, 2)
    sessionAggrStat.step_7_9 = Utils.getScale(Utils.getFieldValue(value, GlobalContants.STEP_7_9, "[|]").toDouble / sessionAggrStat.session_count, 2)
    sessionAggrStat.step_10_30 = Utils.getScale(Utils.getFieldValue(value, GlobalContants.STEP_10_30, "[|]").toDouble / sessionAggrStat.session_count, 2)
    sessionAggrStat.step_30_60 = Utils.getScale(Utils.getFieldValue(value, GlobalContants.STEP_30_60, "[|]").toDouble / sessionAggrStat.session_count, 2)
    sessionAggrStat.step_60 = Utils.getScale(Utils.getFieldValue(value, GlobalContants.STEP_60, "[|]").toDouble / sessionAggrStat.session_count, 2)
    SessionAggrStatDao.deleteByTaskId(task_id)
    SessionAggrStatDao.inserEntity(sessionAggrStat)
  }

  def caculatorCategoryTop5(eventLogRDD: RDD[(String, String, String, Long, String, String, String, String, String,String)], sc: SparkContext, task_id: Int) = {
    //(gid,eventName)
    val gidRDD = eventLogRDD.filter(x => x._9 != null).map(x => (x._9.toLong, x._3))
    //从mysql中读取商品信息（gid,cid） cidRDD
    val cidRDD = new JdbcRDD[(Long, Long)](
      sc,
      () => JdbcManager.getConnection(),
      "select goods_id,cat_id from ecs_goods where goods_id>? and goods_id<?",
      0,
      Long.MaxValue,
      2,
      (resultSet: ResultSet) => {
        (resultSet.getLong("goods_id"), resultSet.getLong("cat_id"))
      }
    )
    //gidRDD.join(cidRDD)====>(gid,(eventName,cid))===Map===>(cid,eventName)
    val tuple2RDD = gidRDD.join(cidRDD).map(x => (x._2._2, x._2._1))
    //获取发生过事件的品类
    val distinctCidRDD = tuple2RDD.map(x => (x._1, x._1)).distinct()
    //获发生过点击事件的品类及其次数
    val clickCountRDD = tuple2RDD.filter(x => x._2.equals(EventEnum.pageViewEvent.toString)).map(x => (x._1, 1)).reduceByKey(_ + _)
    //获取发生过加入购物车事件的品类及其次数
    val cartCountRDD = tuple2RDD.filter(x => x._2.equals(EventEnum.addCartEvent.toString)).map(x => (x._1, 1)).reduceByKey(_ + _)
    //获取发生过下单事件的品类及其次数
    val orderCountRDD = tuple2RDD.filter(x => x._2.equals(EventEnum.orderEvent.toString)).map(x => (x._1, 1)).reduceByKey(_ + _)
    //获取发生支付事件的品类及其次数
    val payCountRDD = tuple2RDD.filter(x => x._2.equals(EventEnum.payEvent.toString)).map(x => (x._1, 1)).reduceByKey(_ + _)

    //(cid,(cid,Some/None))
    val concatRDD = distinctCidRDD.leftOuterJoin(clickCountRDD).map(t2 => {
      var clickCount = 0
      if (!t2._2._2.isEmpty) {
        clickCount = t2._2._2.get
      }
      val click_count = GlobalContants.CLICK_COUNT + "=" + clickCount
      (t2._1, click_count)
    }) //(cid,(click_count=0,Some/None))
      .leftOuterJoin(cartCountRDD).map(t2 => {
      var cartCount = 0
      if (!t2._2._2.isEmpty) {
        cartCount = t2._2._2.get
      }
      val cart_count = t2._2._1 + "|" + GlobalContants.CART_COUNT + "=" + cartCount
      (t2._1, cart_count)
    }) //(cid,(click_count=0|cart_count=0,Some/None))
      .leftOuterJoin(orderCountRDD).map(t2 => {
      var orderCount = 0
      if (!t2._2._2.isEmpty) {
        orderCount = t2._2._2.get
      }
      val order_count = t2._2._1 + "|" + GlobalContants.ORDER_COUNT + "=" + orderCount
      (t2._1, order_count)
    }) //(cid,(click_count=0|cart_count=0|order_count=0,Some/None))
      .leftOuterJoin(payCountRDD).map(t2 => {
      var payCount = 0
      if (!t2._2._2.isEmpty) {
        payCount = t2._2._2.get
      }
      val pay_count = t2._2._1 + "|" + GlobalContants.PAY_COUNT + "=" + payCount
      (t2._1, pay_count)
    })

    val top5Array = concatRDD.map(t2 => {
      val click_count = Utils.getFieldValue(t2._2,GlobalContants.CLICK_COUNT,"[|]").toInt
      val cart_count = Utils.getFieldValue(t2._2,GlobalContants.CART_COUNT,"[|]").toInt
      val order_count = Utils.getFieldValue(t2._2,GlobalContants.ORDER_COUNT,"[|]").toInt
      val pay_count = Utils.getFieldValue(t2._2,GlobalContants.PAY_COUNT,"[|]").toInt
      (new CateGorySecondSort(click_count,cart_count,order_count,pay_count),t2._1)
    }).sortByKey(false).take(5)

    val top5CategoryArray = ArrayBuffer[Top5Category]()
    top5Array.foreach(t2=>{
      val top5Category = new Top5Category(task_id,t2._2,t2._1.click_count,t2._1.cart_count,t2._1.order_count,t2._1.pay_count)
      top5CategoryArray.append(top5Category)
    })
    Top5CategoryDao.deleteByTaskId(task_id)
    if (top5CategoryArray.length>0){
      Top5CategoryDao.insertEntities(top5CategoryArray.toArray)
    }

  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("spark://hadoop1:7077")
    //验证输入参数是否正确
    processArgs(sparkConf, args)
    //去数据库中查找任务对应的信息
    val sparkTask = SparkTaskDao.getSparkTaskById(sparkConf.get(GlobalContants.TASK_RUN_ID).toInt)
    if (sparkTask == null)
      throw new SparkException(s"从数据库中获取任务信息失败")
    //{"startDate":"2018-03-29 00:00:00","endDate":"2018-03-30 00:00:00"}
    val sparkParamObject = JSON.parseObject(sparkTask.task_param)
    //初始化扫描器
    val scan: Scan = initScan(sparkParamObject)
    //接下来需要将原始的scan转换成可以进行base64转码的scan
    val protoScan = ProtobufUtil.toScan(scan)
    val base64Scan = Base64.encodeBytes(protoScan.toByteArray)

    val jobConf = new JobConf(new Configuration())
    //设置要读取的表
    jobConf.set(TableInputFormat.INPUT_TABLE, EventLogContants.HBASE_EVENT_LOG_TABLE)
    //设置扫描器去扫描指定的表
    jobConf.set(TableInputFormat.SCAN, base64Scan)

    val sc = new SparkContext(sparkConf)

    sc.addJar("D:\\IDeaCode\\1705\\jd_bigdata_project\\target\\jd_bigdata_project-1.0-SNAPSHOT.jar")

    val eventLogRDD = sc.newAPIHadoopRDD(jobConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
      .map(_._2).map(result => {
      //用户标识
      val uuid = Bytes.toString(result.getValue(EventLogContants.HBASE_EVENT_LOG_TABLE_FAMILY.getBytes(), EventLogContants.LOG_COLUMN_NAME_UUID.getBytes()))
      //会话id
      val sessionId = Bytes.toString(result.getValue(EventLogContants.HBASE_EVENT_LOG_TABLE_FAMILY.getBytes(), EventLogContants.LOG_COLUMN_NAME_SID.getBytes()))
      //访问时间
      val access_time = Bytes.toString(result.getValue(EventLogContants.HBASE_EVENT_LOG_TABLE_FAMILY.getBytes(), EventLogContants.LOG_COLUMN_NAME_ACCESS_TIME.getBytes())).toLong
      //事件名称
      val eventName = Bytes.toString(result.getValue(EventLogContants.HBASE_EVENT_LOG_TABLE_FAMILY.getBytes(), EventLogContants.LOG_COLUMN_NAME_EVENT_NAME.getBytes()))
      //国家
      val country = Bytes.toString(result.getValue(EventLogContants.HBASE_EVENT_LOG_TABLE_FAMILY.getBytes(), EventLogContants.LOG_COLUMN_NAME_COUNTRY.getBytes()))
      //省份
      val province = Bytes.toString(result.getValue(EventLogContants.HBASE_EVENT_LOG_TABLE_FAMILY.getBytes(), EventLogContants.LOG_COLUMN_NAME_PROVINCE.getBytes()))
      //城市
      val city = Bytes.toString(result.getValue(EventLogContants.HBASE_EVENT_LOG_TABLE_FAMILY.getBytes(), EventLogContants.LOG_COLUMN_NAME_CITY.getBytes()))
      //搜索关键词
      val keyword = Bytes.toString(result.getValue(EventLogContants.HBASE_EVENT_LOG_TABLE_FAMILY.getBytes(), EventLogContants.LOG_COLUMN_NAME_KEYWORD.getBytes()))
      //商品id
      val gid = Bytes.toString(result.getValue(EventLogContants.HBASE_EVENT_LOG_TABLE_FAMILY.getBytes(), EventLogContants.LOG_COLUMN_NAME_GOODS_ID.getBytes()))
      //商品id
      val browserName = Bytes.toString(result.getValue(EventLogContants.HBASE_EVENT_LOG_TABLE_FAMILY.getBytes(), EventLogContants.LOG_COLUMN_NAME_BROWSER_NAME.getBytes()))
      (uuid, sessionId, eventName, access_time, country, province, city, keyword, gid,browserName)
    })
    /*    /**
          * (1, 11, eventName, access_time, country, province, city, keyword, gid)
          * (1, 11, eventName, access_time, country, province, city, keyword, gid)
          * 。。。。。。。。。。。。。。。。。。。。。。。。。。。。。。。。。。。
          * (2, 21, eventName, access_time, country, province, city, keyword, gid)
          * (2, 21, eventName, access_time, country, province, city, keyword, gid)
          *
          *
          * （11，List((1, 11, eventName, access_time, country, province, city, keyword, gid),(1, 11, eventName, access_time, country, province, city, keyword, gid)。。)）
          * （21，List((2, 21, eventName, access_time, country, province, city, keyword, gid),(2, 21, eventName, access_time, country, province, city, keyword, gid)。。)）
          *
          * (1,sid=11|country=china|province=bj|city=bj|visitTimeLength=7|visitStepLegth=8|keyword=xm,hw,pg,mz|goodsId=1,2,3,4|...)
          */
        val sessionGroupRDD = eventLogRDD.groupBy(_._2)

        val sessionFullInfoRDD = sessionFullInfo(sessionGroupRDD)

        //定义一个累加器
        val sessionAggrStatAccumulator = sc.accumulator("")(SessionAggrStatAccumulator)

        sessionFullInfoRDD.foreach(t2 => {
          sessionAggrStatAccumulator.add(GlobalContants.SESSION_COUNT)
          //取出本次session的访问时长和访问步长
          val visiTimeLength = Utils.getFieldValue(t2._2, GlobalContants.SESSION_VISIT_TIME_LENGTH, "[|]").toInt
          val visiStepLength = Utils.getFieldValue(t2._2, GlobalContants.SESSION_VISIT_STEP_LENGTH, "[|]").toInt
          //计算访问时长和访问步长所属区间范围，然后累加器对应的字段+1
          caculatorVisisTimeLengthRange(visiStepLength, sessionAggrStatAccumulator)
          caculatorVisitStepLengthRange(visiStepLength, sessionAggrStatAccumulator)
        })
        //session_count=15|1s_3s=7|4s_6s=0|7s_9s=0|10s_30s=2|30s_60s=2|
        // 1m_3m=1|3m_10m=2|10m_30m=1|30m=0|1_3=12|4_6=2|7_9=0|10_30=1|30_60=0|60=0
        saveResultToMysql(sessionAggrStatAccumulator.value,sparkTask.task_id)
        */
    //在符合条件的session中，获取点击、加入购物车，下单，支付数量排名前5的品类
    caculatorCategoryTop5(eventLogRDD, sc, sparkTask.task_id)

    //统计当天每个省份的新增用户数
    eventLogRDD.filter(x => x._3.equals(EventEnum.launchEvent.toString)).map(x => (x._6, 1)).reduceByKey(_ + _).foreach(println(_))
    //统计当天每个省份访客数
    eventLogRDD.map(x => (x._6, x._1)).distinct().map(x => (x._1, 1)).reduceByKey(_ + _).foreach(println(_))
    //按浏览器维度统计web端活跃用户数
    eventLogRDD.map(x=>(x._10,x._1)).distinct().map(x => (x._1, 1)).reduceByKey(_ + _).foreach(println(_))
    sc.stop()
  }
}
