package com.daoke360.task.review

import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkStreamingDemo {

  //调整日志级别
  Logger.getLogger("org").setLevel(Level.WARN)
  val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
  val sc = new SparkContext(sparkConf)
  /**
    * 开始创建StreamingContext，并对StreamingContext进行初始化，在初始化的时候，会创建一个特别
    * 重要的对象JobScheduler(任务调度)，同时在初始化这个JobScheduler时，又会创建一个特别重要的
    * 对象JobGenerator(生成任务)
    */
  val ssc = new StreamingContext(sc,Seconds(2))
  //指定checkpoint路径
  ssc.checkpoint("/checkpoint")
  //创建一个输入的dstream
  val socketInputDstream = ssc.socketTextStream("hadoop1",8989)
  //对输入的dstream进行不断地transform转换操作，形成一个有一个的dstream
  val wordsDstream = socketInputDstream.flatMap(_.split(" "))
  val tuple2Dstream = wordsDstream.map((_,1))
  val wordCountDstream = tuple2Dstream.reduceByKey(_+_)

  //可以对多次引用道德dstream进行持久化
  wordCountDstream.persist(StorageLevel.MEMORY_ONLY_SER)
  /**
    * 也可以对一个dstream进行checkpoint
    * 一般来书不要过于频繁进行ckckpoint，因为在进行checkpoint的时候要耗费一定时间将相当
    * 一部分数据写入到第三个高可靠的文件系统中去，这样会降低性能和作业的吞吐量
    * 注意：checkpoint 的时间间隔一定要是batch产生的时间间隔的整数倍，
    * spark官方建议你不要低于10s
    */
  wordCountDstream.checkpoint(Seconds(20))
  //对最后的dstr调用output算子触发任务的调度，将结果保存起来
  wordCountDstream.print()
  wordCountDstream.print()
  wordCountDstream.print()

  /**
    * Driver
    * 调用start方法，启动这个spark streaming应用
    * ===》在这个过程中会创建一个特别重要的对象DstreamGraph（保存要创建输入的dstream及其转换关
    * 系及其对应的将来要启动的Reveivers）
    * ===》调用JobScheduler的start方法创建一个重量级对象ReveiverTracker
    * ===》调用ReveiverTracker。start（）方法
    * ===》创建一个发射器ReceiverTrackerEndpoint
    * ===》从DsreamGraph获取receivers，然后将这些reveivers封装到一个case样例类StartAllReceivers中
    * ===》调用发射器ReceiverTrackerEndpoint的send方法发给自己（StartAllReceiver方法）
    * ===》在StartAllReceivers这个方法中会用startReveiver方法
    * ===》在startReceiver方法中，最终会滴啊用sparkContext的submitjob方法，将要启动的Receiver以
    * 任务的形式发送给Executor
    *
    * Executor
    * ===》将接受的的任务（启动receiver）封装成ReceiverSupervisorImpl
    * ===》调用ReceiverSupervisorImpl的start方法，在这个start方法中调用给的onStart()和startReceiver()方法
    * ===》在这个onStart()方法中，创建一个用存放Receiver接受到的数据的对象BlockFenerator
    * ===》在这个startReceiver()方法中真正启动了接收数据的Receiver
     */
  ssc.start()
  //将sparkstreaming陈旭祖册在这里，等待停止命令
  ssc.awaitTermination()

}
