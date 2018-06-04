package com.daoke360.task.analysislog

import com.daoke360.caseclass.IPRule
import com.daoke360.utils.Utils
import com.daoke360.common.{EventLogContants, GlobalContants}
import com.daoke360.task.analysislog.utils.LogAnalysisUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.{SparkConf, SparkContext, SparkException}

object AnalysisLogTask {
  /**
    * 验证输入参数是否正确
    *
    * @param args
    * @param sparkConf
    */
  def processArgs(args: Array[String], sparkConf: SparkConf) = {
    if (args.length >= 2 && args(0).equals(GlobalContants.TASK_PARAMS_FLAG) && Utils.validateInpuDate(args(1))) {
      sparkConf.set(GlobalContants.TASK_RUN_DATE, args(1))
    } else {
      throw new SparkException(
        """
          |At least two parameters are required for the task example: xx.jar -d yyyy-MM-dd
          |<-d>:Marking of the start of task parameters
          |<yyyy-MM-dd>:Task run date
        """.stripMargin)
    }

  }

  /**
    * 处理输入路径
    * /logs/2018/03/29
    *
    * @param sparkConf
    * @return
    */
  def processInputPath(sparkConf: SparkConf) = {
    var fs: FileSystem = null
    try {
      //将字符串日期转换成long类型的时间戳
      val longTime = Utils.parseDate2Long(sparkConf.get(GlobalContants.TASK_RUN_DATE), "yyyy-MM-dd")
      //将时间戳转换成指定格式的日期
      val inputDate = Utils.formatDate(longTime, "yyyy/MM/dd")
      val inputPath = new Path(GlobalContants.LOG_DIR_PREFIX + inputDate)
      fs = FileSystem.newInstance(new Configuration())
      if (fs.exists(inputPath)) {
        sparkConf.set(GlobalContants.TASK_INPUT_PATH, inputPath.toString)
      } else {
        throw new Exception("not found input path of task....")
      }
    } catch {
      case e: Exception => throw e
    } finally {
      if (fs != null) {
        fs.close()
      }
    }

  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .setMaster("local")

    val jobConf = new JobConf(new Configuration())
    //指定输出的类，这个类专门用来将spark处理的结果写入到hbase中
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    //指定要将数据写入到哪张表
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, EventLogContants.HBASE_EVENT_LOG_TABLE)
    //验证输入参数是否正确
    processArgs(args, sparkConf)
    //处理输入路径
    processInputPath(sparkConf)

    val sc = new SparkContext(sparkConf)
    // sc.addJar("G:\\00beicaishixun\\IdeaWorkSpace\\Spark\\student\\1705\\jd_bigdata_project\\target\\jd_bigdata_project-1.0-SNAPSHOT.jar")

    //加载ip规则库
    /**
      * collect 作用：
      * 1，触发任务的提交
      * 2，将executor上的任务处理结果，拉回到driver
      */
    val ipRules: Array[IPRule] = sc.textFile("/home/in/ip.data").map(line => {
      val fields = line.split("\\|")
      IPRule(fields(2).toLong, fields(3).toLong, fields(5), fields(6), fields(7))
    }).collect()

    val ipRulesBroadCast = sc.broadcast(ipRules)


    //加载hdfs上的日志
    /**
      * 定义在算子外面的变量称之为外部变量，外部变量都是在driver上定义的
      */
    val eventLogMap = sc.textFile(sparkConf.get(GlobalContants.TASK_INPUT_PATH)).map(logText => {
      LogAnalysisUtils.analysisLog(logText, ipRulesBroadCast.value)
    }).filter(m => m != null)

    val tuple2RDD = eventLogMap.map(map => {
      /**
        * 构建rowkey原则：
        * 1，唯一性 2，散列 3，长度不能过长，4，方便查询
        *
        * access_time+"_"+ (uuid+enventName).hashcode
        */
      //用户访问时间
      val accessTime = map(EventLogContants.LOG_COLUMN_NAME_ACCESS_TIME)
      //用户唯一标识
      val uuid = map(EventLogContants.LOG_COLUMN_NAME_UUID)
      //事件名称
      val eventName = map(EventLogContants.LOG_COLUMN_NAME_EVENT_NAME)
      //构建rowkey
      val rowKey = accessTime + "_" + Math.abs((uuid + eventName).hashCode)
      //构建put对象
      val put = new Put(rowKey.getBytes())
      map.foreach(t2 => {
        put.addColumn(EventLogContants.HBASE_EVENT_LOG_TABLE_FAMILY.getBytes(), t2._1.getBytes(), t2._2.getBytes())
      })
      //保存到hbase中的数据一定要是对偶元组格式的
      (new ImmutableBytesWritable(), put)
    })
    //将结果保存到hbase中      create 'event_log','log'
    tuple2RDD.saveAsHadoopDataset(jobConf)
    sc.stop()
  }

}
