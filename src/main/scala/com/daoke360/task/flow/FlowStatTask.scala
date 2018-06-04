package com.daoke360.task.flow

import com.daoke360.caseclass.IPRule
import com.daoke360.common.{EventLogContants, GlobalContants}
import com.daoke360.dao.{DimensionDao, StatsLocationFlowDao}
import com.daoke360.domain.{DateDimension, LocationDimension, StatsLocationFlow}
import com.daoke360.enum.EventEnum
import com.daoke360.jdbc.JdbcManager
import com.daoke360.task.analysislog.utils.LogAnalysisUtils
import com.daoke360.task.flow.kafkaManager.KafkaManager
import com.daoke360.utils.Utils
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object FlowStatTask {
  val checkpointPath = StreamingConstants.checkpointPath + System.currentTimeMillis()


  def createStreamingContext(sc: SparkContext, ipRulesBroadCast: Broadcast[Array[IPRule]]) = {
    val ssc = new StreamingContext(sc, Seconds(StreamingConstants.batchInterval))
    //设置checkpoint目录
    ssc.checkpoint(checkpointPath)

    val kafkaManager = new KafkaManager(StreamingConstants.kafkaParams, Set(StreamingConstants.kafkaTopic))
    //创建输入的dstream
    val kafkaInputDstream = kafkaManager.createDirectStream[String, String, StringDecoder, StringDecoder, String](ssc)

    val eventLogMapDstream = kafkaInputDstream.map(logText => {
      val eventLogMap = LogAnalysisUtils.analysisLog(logText, ipRulesBroadCast.value)
      eventLogMap
    }).filter(x => x != null && x.size > 0)

    eventLogMapDstream.map(eventLogMap => {
      //访问时间
      val access_time = Utils.formatDate(eventLogMap(EventLogContants.LOG_COLUMN_NAME_ACCESS_TIME).toLong, "yyyy-MM-dd")
      //地区信息
      val country = eventLogMap(EventLogContants.LOG_COLUMN_NAME_COUNTRY)
      val province = eventLogMap(EventLogContants.LOG_COLUMN_NAME_PROVINCE)
      val city = eventLogMap(EventLogContants.LOG_COLUMN_NAME_CITY)
      //事件名称
      val eventName = eventLogMap(EventLogContants.LOG_COLUMN_NAME_EVENT_NAME)
      //用户标识
      val uuid = eventLogMap(EventLogContants.LOG_COLUMN_NAME_UUID)
      //会话标识
      val sid = eventLogMap(EventLogContants.LOG_COLUMN_NAME_SID)
      ((access_time, country, province, city), (eventName, uuid, sid))
    }).flatMap(t2 => {
      Array(
        //全国
        ((t2._1._1, t2._1._2, GlobalContants.VALUE_OF_ALL, GlobalContants.VALUE_OF_ALL), t2._2),
        //省
        ((t2._1._1, t2._1._2, t2._1._3, GlobalContants.VALUE_OF_ALL), t2._2),
        //城市
        ((t2._1._1, t2._1._2, t2._1._3, t2._1._4), t2._2)
      )
    }) //（（2018-04-16 country province city）， Array(HashSet(uuid),HashMap(sid->count),0,0)）
      .updateStateByKey((it: Iterator[((String, String, String, String), Seq[(String, String, String)], Option[Array[Any]])]) => {
      it.map(t3 => {
        //取出原来聚合的结果
        val array = t3._3.getOrElse(Array(new mutable.HashSet[String](), new mutable.HashMap[String, Int](), 0, 0))
        val uuidSet = array(0).asInstanceOf[mutable.HashSet[String]]
        val sidMap = array(1).asInstanceOf[mutable.HashMap[String, Int]]
        var nu = array(2).asInstanceOf[Int]
        var pv = array(3).asInstanceOf[Int]

        t3._2.foreach(tuple3 => {
          //tuple3===>(eventName, uuid, sid)
          val uuid = tuple3._2
          uuidSet.add(uuid)

          val sid = tuple3._3
          sidMap.put(sid, sidMap.getOrElse(sid, 0) + 1)

          val eventName = tuple3._1
          if (eventName.equals(EventEnum.launchEvent.toString))
            nu += 1

          if (eventName.equals(EventEnum.pageViewEvent.toString))
            pv += 1
        })

        array(0) = uuidSet
        array(1) = sidMap
        array(2) = nu
        array(3) = pv
        (t3._1, array)
      })
    }, new HashPartitioner(sc.defaultParallelism), true).foreachRDD(rdd => {

      //注意：连接对象不能在此处创建，因为在这里创建那么这个连接对象相当于是一个外部变量，外部变量需要使用的话首先应该进行序列化，
      //但是连接对象是不能被序列化的。所以在此处创建连接对象抛异常

      if (!rdd.isEmpty()) {
        rdd.foreachPartition(partition => {
          //在此处创建连接对象，性能最佳
          val connection = JdbcManager.getConnection()
          val statsLocationFlowBuffer = ArrayBuffer[StatsLocationFlow]()
          //yyyy-MM-dd===longTime=>时间维度对象 new DateDimension(year,season,month,week....)
          //country,province,city==>地域维度 new LocationDimension(country,province,city)
          partition.foreach(t2 => {
            //不能在此处创建连接对象，虽然可以使用不会抛异常，但是在这里创建意味着每遍历一条数据就会创建一个连接对象，性能极其差劲

            //t2==>((access_time,country,province,city), Array(HashSet(uuid),HashMap(sid->count),0,0)）)
            val key = t2._1
            val date = key._1
            val longTime = Utils.parseDate2Long(date, "yyyy-MM-dd")
            //构建时间维度
            val dateDimension = DateDimension.buildDateDimension(longTime)
            //获取时间维度id
            val date_dimension_id = DimensionDao.getDimensionId(dateDimension, connection)

            val country = key._2
            val province = key._3
            val city = key._4
            //构建地域维度
            val locationDimension = new LocationDimension(country, province, city)
            //获取地域维度id
            val location_dimension_id = DimensionDao.getDimensionId(locationDimension, connection)

            val array = t2._2
            val uuidSet = array(0).asInstanceOf[mutable.HashSet[String]]
            //访客数，uv
            val uv = uuidSet.size
            val sidMap = array(1).asInstanceOf[mutable.HashMap[String, Int]]
            //会话个数
            val sn = sidMap.size
            //跳出数 on
            var on = 0
            sidMap.foreach(t2 => if (t2._2 == 1) on += 1)
            //新增用户
            var nu = array(2).asInstanceOf[Int]
            //页面流量量
            var pv = array(3).asInstanceOf[Int]
            statsLocationFlowBuffer += new StatsLocationFlow(date_dimension_id, location_dimension_id, nu, uv, pv, sn, on, date)
          })
          if (statsLocationFlowBuffer.size > 0)
            StatsLocationFlowDao.update(statsLocationFlowBuffer.toArray)
          JdbcManager.returnConnection(connection)
        })
        kafkaManager.updateConsumeOffset()
      }
    })
    ssc
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    //加载ip规则库
    val ipRules: Array[IPRule] = sc.textFile("/spark_sf_project/resource/ip.data").map(line => {
      val fields = line.split("\\|")
      IPRule(fields(2).toLong, fields(3).toLong, fields(5), fields(6), fields(7))
    }).collect()

    val ipRulesBroadCast = sc.broadcast(ipRules)


    val ssc = StreamingContext.getOrCreate(checkpointPath, () => createStreamingContext(sc, ipRulesBroadCast))
    ssc.start()
    ssc.awaitTermination()
  }

}
