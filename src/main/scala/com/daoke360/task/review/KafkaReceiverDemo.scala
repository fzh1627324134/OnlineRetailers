package com.daoke360.task.review

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Receiver方式有以下特点:
  * 1.有Receiver这个组件,Receiver会独占凑core,知道程序停止才会被释放
  * 2.使用的是kafka高层次API,消费偏移量有高阶API去维护,sparkstreaming不管理消费偏移量
  * 3.Receiver接收到数据后形成的rdd的分区数和topic分区数没有任何关系,即分区数不是一一对应的关系
  *
  * 启动kafka命令
  * /opt/apps/kafka/bin/kafka-server-start.sh  -daemon /opt/apps/kafka/config/server.properties
  * ##查看所有topic##
  * /opt/apps/kafka/bin/kafka-topics.sh --list --zookeeper  hadoop1:2181
  * ##创建topic##
  * /opt/apps/kafka/bin/kafka-topics.sh --create --zookeeper hadoop1:2181 --replication-factor 1 --partitions 3 --topic wordcount
  * ##客户日志生产者##
  * /opt/apps/kafka/bin/kafka-console-producer.sh --broker-list hadoop1:9092 --topic wordcount
  * ##客户日志消费者##
  * /opt/apps/kafka/bin/kafka-console-consumer.sh --zookeeper hadoop1:2181  --topic wordcount
  */
object KafkaReceiverDemo {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    //设置Receiver接手速度
    //如果集群资源有限,并没有大到足以让应用程序一接收到数据就立即处理它(处理速度跟不上数据的接收速度)
    //Receiver可以被设置一个最大接收限速,以每秒接收多少条单位来限速
    //SparkConf.set("spark.streaming.backpressure.enabled","true")

    /**
      * 开启预写日志,保证数据零丢失,此时需要注意将我们的dstream的存储级别调整一下SorageLevel.MEMORY_ONLY
      * 然而,这种极强的可靠性机制，会导致Receiver的吞吐量大幅度下降，因为单位时间内，有相当一部分时间需要
      * 将数据写入预写日志
      * 如果又希望开启预写日志机制，确保数据零丢失，又不希望影响系统的吞吐量，那么可以创建多个输入DStream，
      * 启动多个Rceiver
      * 此外，在启用预写机制之后，推荐将复制持久化机制禁用掉，因为所有数据已经保存在容错的文件系统中了,
      * 不需要再用复制机制进行持久化，保存一份副本了，只要将输入DStream的持久化机制设置一下即可，
      * persist(StorageLevel.MEMORY_AND_DISK_SER)
      */
    sparkConf.set("spark.streaming.reveiver.writeAheadLog.enable", "true")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(2))
    //开启预写日志需要制定预写目录
    ssc.checkpoint("/checkpoint1705")
    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> "hadoop1:2181,hadoop2:2181",
      "group.id" -> "w1705w",
      //在receiver模式下，多久提交一次消费偏移量到zk上，默认是60*1000（即60秒）
      "auto.commit.interval.ms" -> "1000"
    )
    /**
      * //创建数据的dstream
      * val kafkaInputDstream = KafkaUtils.createStream[String,String,StringDecoder,
      * StringDecoder](ssc,kafkaParams,Map("wordcount" -> 3),StorageLevel.MEMORY_AND_DISK)
      * .map(_._2)
      * val wordCountDstream = kafkaInputDstream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
      *wordCountDstream.print()
      */
    val kafkaInputDstreams = (0 to 2).map(x => {
      val kafkaInputDstream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, Map("wordcount" -> 3), StorageLevel.MEMORY_AND_DISK).map(_._2)
      kafkaInputDstream
    })
    val kafkaInputUntionDstream = ssc.union(kafkaInputDstreams)
    val wordCountDstream = kafkaInputUntionDstream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    wordCountDstream.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
