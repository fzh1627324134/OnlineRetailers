package com.daoke360.task.product

import com.alibaba.fastjson.{JSON, JSONObject}
import com.daoke360.common.{EventLogContants, GlobalContants}
import com.daoke360.dao.{AreaTop3ProductDao, SparkTaskDao}
import com.daoke360.domain.AreaTop3Product
import com.daoke360.enum.EventEnum
import com.daoke360.utils.Utils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkException}
import scala.collection.mutable.ArrayBuffer

object AreaTop3ProductTask {

  /**
    * 校验输入参数是否正确
    * @param args
    * @param sparkConf
    */
  def processArgs(args: Array[String], sparkConf: SparkConf) = {
    if (args.length >= 2 && args(0).equals(GlobalContants.TASK_PARAMS_FLAG) && Utils.validateNumber(args(1))) {
      sparkConf.set(GlobalContants.TASK_RUN_ID, args(1))
    } else {
      throw new SparkException("输入参数有误...")
    }
  }

  /**
    * 初始化扫描器
    * @param taskParamObject
    */
  def initScan(taskParamObject: JSONObject) = {
    //取出开始时间和结束时间
    val startDate = taskParamObject.getString(GlobalContants.TASK_PARAM_START_DATE)
    val endDate = taskParamObject.getString(GlobalContants.TASK_PARAM_END_DATE)
    //将指定格式的日期转换成时间戳
    val startTime = Utils.parseDate2Long(startDate, "yyyy-MM-dd HH:mm:ss").toString
    val endTime = Utils.parseDate2Long(endDate, "yyyy-MM-dd HH:mm:ss").toString
    val scan = new Scan()
    //设置扫描器的起始位置
    scan.setStartRow(startTime.getBytes())
    scan.setStopRow(endTime.getBytes())
    scan
  }

  /**
    * 将rdd转换成dataframe
    *
    * @param eventLogRDD (eventName, province, city, gid)
    * @param sparkSession
    */
  def registAreaProductClickInfoTmp(eventLogRDD: RDD[(String, String, String, String)], sparkSession: SparkSession) = {
    val rowRDD = eventLogRDD.map(t4 => Row(t4._2, t4._3, t4._4.toInt))

    //定义元数据信息
    val schema = StructType(List(
      StructField("province", StringType, true),
      StructField("city", StringType, true),
      StructField("gid", IntegerType, true)
    ))
    //将rowRDD和元数据信息schema进行关联
    val dataFrame = sparkSession.createDataFrame(rowRDD, schema)

    //将dataframe注册成一个视图
    dataFrame.createOrReplaceTempView("area_product_click_info_tmp")
  }

  /**
    * 统计每个地区对每个商品的点击次数
    *
    * @param sparkSession
    * @return
    */
  def registAreaProductClickCountInfoTmp(sparkSession: SparkSession) = {
    sparkSession.sql(
      """
        |select province,city,gid, count(*) click_count from area_product_click_info_tmp
        |group by province,city,gid
      """.stripMargin).createOrReplaceTempView("area_product_click_count_tmp")
  }

  /**
    * 统计每个地区对每个商品总的点击次数
    *
    * @param sparkSession
    * @return
    */
  def registAreaProductClickTotalCountInfoTmp(sparkSession: SparkSession) = {
    //注册用户自定义聚合函数
    sparkSession.udf.register("city_concat", new CityConCatUDAF())
    sparkSession.sql(
      """
        |select  province,gid,sum(click_count),city_concat(city) from area_product_click_count_tmp
        |group by   province,gid
      """.stripMargin).show()
  }

  def caculatorAreaProductClickCountTop3(sparkSession: SparkSession, task_id: Int) = {
    val resultDataFrame = sparkSession.sql(
      """
        |select province,gid,click_count,cities
        |from(
        |select row_number()over(partition by province order by click_count desc) rank,province,gid,click_count,cities
        |from area_product_click_total_count_tmp) tmp
        |where rank<=3
      """.stripMargin)
    val areaTop3ProductBuffer = ArrayBuffer[AreaTop3Product]()
    resultDataFrame.collect().foreach(row => {
      val areaTop3Product = new AreaTop3Product(
        task_id,
        row.getAs[String]("province"),
        row.getAs[Int]("gid"),
        "",
        row.getAs[Long]("click_count"),
        row.getAs[String]("cities")
      )
      areaTop3ProductBuffer += areaTop3Product
    })
    AreaTop3ProductDao.deleteByTaskId(task_id)
    if (areaTop3ProductBuffer.size > 0)
      AreaTop3ProductDao.insertEntities(areaTop3ProductBuffer.toArray)
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    //校验输入参数是否正确
    processArgs(args, sparkConf)
    //从mysql中获取任务信息
    val sparkTask = SparkTaskDao.getSparkTaskById(sparkConf.get(GlobalContants.TASK_RUN_ID).toInt)
    if (sparkTask == null)
      throw new SparkException("从数据库中获取任务信息失败...")

    val taskParamObject = JSON.parseObject(sparkTask.task_param)
    //初始化扫描器
    val scan = initScan(taskParamObject)
    //将scan转换成字符串
    val protoScan = ProtobufUtil.toScan(scan)
    val base4StringScan = Base64.encodeBytes(protoScan.toByteArray)
    //创建配置对象
    val jobConf = new JobConf(new Configuration())
    //指定输入的数据源
    jobConf.set(TableInputFormat.INPUT_TABLE, EventLogContants.HBASE_EVENT_LOG_TABLE)
    //指定扫描器
    jobConf.set(TableInputFormat.SCAN, base4StringScan)
    /**
      * 创建一个sparkSession对象
      * SparkSession这个对象是在spark2.0开始出现的，它的出现是为了简便开发和降低spark的学习成本
      * 它封装了SparkContext对象，同时整合了SQLContext和HiveContext对象
      */
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    //去读取hbase中的数据
    val eventLogRDD = sparkSession.sparkContext.newAPIHadoopRDD(jobConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result]).map(_._2)
      .map(result => {
        val eventName = Bytes.toString(result.getValue(EventLogContants.HBASE_EVENT_LOG_TABLE_FAMILY.getBytes(), EventLogContants.LOG_COLUMN_NAME_EVENT_NAME.getBytes()))
        val province = Bytes.toString(result.getValue(EventLogContants.HBASE_EVENT_LOG_TABLE_FAMILY.getBytes(), EventLogContants.LOG_COLUMN_NAME_PROVINCE.getBytes()))
        val city = Bytes.toString(result.getValue(EventLogContants.HBASE_EVENT_LOG_TABLE_FAMILY.getBytes(), EventLogContants.LOG_COLUMN_NAME_CITY.getBytes()))
        val gid = Bytes.toString(result.getValue(EventLogContants.HBASE_EVENT_LOG_TABLE_FAMILY.getBytes(), EventLogContants.LOG_COLUMN_NAME_GOODS_ID.getBytes()))
        (eventName, province, city, gid)
      }).filter(x => x._1.equals(EventEnum.addCartEvent.toString) && x._4 != null)

    //将eventLogRDD转换成DataFrame
    registAreaProductClickInfoTmp(eventLogRDD, sparkSession)
    //统计每个地区对每个商品的点击次数
    registAreaProductClickCountInfoTmp(sparkSession)
    //统计每个省份对每个商品的点击总次数
    registAreaProductClickTotalCountInfoTmp(sparkSession)
    //计算每个省份点击排名前三的商品
    caculatorAreaProductClickCountTop3(sparkSession, sparkTask.task_id)

    sparkSession.stop()
  }

}