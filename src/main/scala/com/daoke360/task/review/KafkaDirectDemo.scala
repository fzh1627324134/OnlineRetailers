package com.daoke360.task.review

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Direct方式没有一下特点:
  * 1.没有Receiver组件,不存在Receiver独占cpu core的问题
  * 2.使用的是kafka低阶API,消费偏移量有Sparkstreaming应用来自己管理
  * 3.所形成的rdd分区数与topic的分区数是一一对应的关系
  */
object KafkaDirectDemo {

  val checkpointPath = "/checkpoint170517052"

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    /**val ssc = new StreamingContext(sc, Seconds(2))
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> "hadoop1:9092,hadoop2:9092",
      "group.id" -> "w1705w"
    )
    //创建输入的dstream
    val kafkaInputDstream = KafkaUtils.createDirectStream[String,String,StringDecoder
      ,StringDecoder](ssc,kafkaParams,Set("wordcount")).map(_._2)
    val wordCountDstream = kafkaInputDstream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    wordCountDstream.print()
    ssc.start()
    ssc.awaitTermination()
      */
    /**
      * 还原点目录保存一下信息:
      * 1.消费偏移量
      * 2.sparkStreaming应用配置信息,即sparkConf
      * 3.保存未处理完的batch
      * 4.dstream的操作转换关系
      */
    val ssc = StreamingContext.getOrCreate(checkpointPath,() => {
      val kafkaParams = Map[String,String](
        "metadata.broker.list" -> "hadoop1:9092,hadoop2:9092",
        "group.id" -> "w1705w"
      )
      val ssc = new StreamingContext(sc,Seconds(2))
      ssc.checkpoint(checkpointPath)
      val kafkaInputDstream = KafkaUtils.createDirectStream[String,String,StringDecoder,
        StringDecoder](ssc,kafkaParams,Set("wordcount")).map(_._2)
      val wordCountDstream = kafkaInputDstream.flatMap(_.split(" ")).map((_,1))
        wordCountDstream.print()
      ssc
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
