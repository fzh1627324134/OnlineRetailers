package com.practice.afirstTimes

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext, SparkException}

object FAnalysisLogTask {

  /**
    * 验证输入参数是否正确
    */
  def processArgs(args:Array[String],sparkConf: SparkConf) = {
    if (args.length >= 2 && args(0).equals(FGlobalContants.TASK_PARAMS_FLAG) && FUtils.validateInpuDate(args(1))){
      sparkConf.set(FGlobalContants.TASK_RUN_DATE,args(1))
    }else{
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
    */
  def processInputPath(sparkConf: SparkConf) = {
    var fs:FileSystem = null
    try{
      val longTime = FUtils.parseDate2Long(sparkConf.get(FGlobalContants.TASK_RUN_DATE),"yyyy-MM-dd")
      val inputDate = FUtils.formtDate(longTime,"yyyy/MM/dd")
      val inputPath = new Path(FGlobalContants.LOG_DIR_PREFIX + inputDate)
      fs = FileSystem.newInstance(new Configuration())
      if (fs.exists(inputPath)){
        sparkConf.set(FGlobalContants.TASK_INPUT_PATH,inputPath.toString)
      }else{
        throw new Exception("not found input path of task...")
      }
    }catch {
      case e:Exception => throw e
    }finally {
      if (fs != null){
        fs.close()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val jobConf = new JobConf(new Configuration())
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,FEventLogContants.HBASE_EVENT_LOG_TABLE)
    processArgs(args,sparkConf)
    processInputPath(sparkConf)
    val sc = new SparkContext(sparkConf)
    val ipRules:Array[FIPRule] = sc.textFile("").map(line =>{
      val fileds = line.split("\\|")
      FIPRule(fileds(2).toLong,fileds(3).toLong.toLong,fileds(5),fileds(6),fileds(7))
    }).collect()
    val ipRuleBroadCast = sc.broadcast(ipRules)
    val eventLogMap = sc.textFile(sparkConf.get(FGlobalContants.TASK_INPUT_PATH)).map(logText => {
      FLogAnalysisUtils.analysisLog(logText,ipRuleBroadCast.value)
    }).filter(m => m != null)
    val tuple2RDD = eventLogMap.map(map => {
      val accessTime = map(FEventLogContants.LOG_COLUMN_NAME_ACCESS_TIME)
      val uuid = map(FEventLogContants.LOG_COLUMN_NAME_UUID)
      val eventName = map(FEventLogContants.LOG_COLUMN_NAME_EVENT_NAME)
      val rowKey = accessTime + "_" + Math.abs((uuid + eventName).hashCode)
      val put = new Put(rowKey.getBytes())
      map.foreach(t2 => {
        put.addColumn(FEventLogContants.HBASE_EVENT_LOG_TABLE_FAMILY.getBytes(),t2._1.getBytes(),t2._2.getBytes())
      })
      (new ImmutableBytesWritable(),put)
    })
    tuple2RDD.saveAsHadoopDataset(jobConf)
    sc.stop()
  }

}
