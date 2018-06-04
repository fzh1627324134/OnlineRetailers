package com.daoke360.task.flow.kafkaManager

import java.util.concurrent.atomic.AtomicReference
import com.daoke360.common.GlobalContants
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import org.apache.spark.SparkException
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import scala.collection.mutable
import scala.reflect.ClassTag
/**
  * 维护消费偏移量
  */
class KafkaManager(var kafkaParams: Map[String, String], topicSet: Set[String]) extends Serializable{
  //创建一个操作kafka数据的集群客户端
  val kafkaCluster = new KafkaCluster(kafkaParams)

  setOrUpdateConsumeOffset()




  /**
    * 设置或更新消费偏移量
    *
    * @return
    */
  def setOrUpdateConsumeOffset() = {

    //定义一个是否消费过的标记，默认为true
    var isConsume = true

    //尝试获取topic的partiton元数据信息
    val errOrTopicAndPartitions = kafkaCluster.getPartitions(topicSet)

    //判断是否获取到topic的partiton元数据信息
    if (errOrTopicAndPartitions.isLeft) {
      throw new SparkException("尝试获取topic的元数据信息失败....")
    }
    //取出topic的prtition的元数据信息
    val topicAndPartitions: Set[TopicAndPartition] = errOrTopicAndPartitions.right.get

    //尝试获取每个分区最早一条消息的偏移量
    val errOrearliestLeaderOffsets = kafkaCluster.getEarliestLeaderOffsets(topicAndPartitions)
    if (errOrearliestLeaderOffsets.isLeft) {
      throw new SparkException("尝试获取topic的最早一条消息的偏移量失败....")
    }
    //取出每个分区最早一条消息的偏移量
    /**
      * Map(
      * event_log_0---->400
      * event_log_1---->400
      * )
      */
    val earliestLeaderOffsetMap: Map[TopicAndPartition, LeaderOffset] = errOrearliestLeaderOffsets.right.get

    //尝试获取每个分区的消费偏移量
    val errOrPartitionToLong = kafkaCluster.getConsumerOffsets(kafkaParams(GlobalContants.GROUP_ID), topicAndPartitions)
    if (errOrPartitionToLong.isLeft) {
      isConsume = false
    }
    //定义一个容器用于存放需要设置或更新消费偏移量的partiton
    val setOrUpdateMap = mutable.Map[TopicAndPartition, Long]()

    //以前消费过
    if (isConsume) {
      //取出消费偏移量
      /**
        * Map(
        * event_log_0----->200
        * event_log_1----->200
        * )
        */
      val consumeOffsetsMap = errOrPartitionToLong.right.get

      consumeOffsetsMap.foreach(t2 => {
        //获取当前topic的partition的名称
        val currentPartition = t2._1
        //取出当前消费偏移量
        val currentOffset = t2._2
        //取出当前这个分区最早一条消息的偏移量
        val earliestOffset = earliestLeaderOffsetMap(currentPartition).offset


        if (currentOffset < earliestOffset) {
          //消费偏移量过时了
          setOrUpdateMap.put(currentPartition, earliestOffset)
        }
      })
    } else {
      //以前没有消费过
      if (kafkaParams(GlobalContants.AUTO_OFFSET_RESET).equals("smallest")) {
        earliestLeaderOffsetMap.foreach(t2 => {
          //smallest
          //获取当前topic的partition的名称
          val currentPartition = t2._1
          //取出当前这个分区最早一条消息的偏移量
          val earliestOffset = t2._2.offset
          setOrUpdateMap.put(currentPartition, earliestOffset)
        })
      } else {
        //largest
        //尝试获取最大消息偏移量
        val errOrPartitionToOffset = kafkaCluster.getLatestLeaderOffsets(topicAndPartitions)
        if (errOrPartitionToOffset.isLeft) {
          throw new SparkException("尝试获取最大消息偏移量失败...")
        }
        val latestLeaderOffsetsMap = errOrPartitionToOffset.right.get
        latestLeaderOffsetsMap.foreach(t2 => {
          //获取当前topic的partition的名称
          val currentPartition = t2._1
          //取出当前这个分区最早一条消息的偏移量
          val latestLeaderOffset = t2._2.offset
          setOrUpdateMap.put(currentPartition, latestLeaderOffset)
        })
      }
    }
    //设置或更新消费偏移量
    kafkaCluster.setConsumerOffsets(kafkaParams(GlobalContants.GROUP_ID), setOrUpdateMap.toMap)
  }


  /**
    * 获取消费偏移量
    */
  def getConsumeOffset(): Map[TopicAndPartition, Long] = {
    //尝试获取topic的partiton元数据信息
    val errOrTopicAndPartitions = kafkaCluster.getPartitions(topicSet)

    //判断是否获取到topic的partiton元数据信息
    if (errOrTopicAndPartitions.isLeft) {
      throw new SparkException("尝试获取topic的元数据信息失败....")
    }
    //取出topic的prtition的元数据信息
    val topicAndPartitions: Set[TopicAndPartition] = errOrTopicAndPartitions.right.get
    //尝试获取每个分区最大一条消息的偏移量
    val errOrConsumeOffsets = kafkaCluster.getConsumerOffsets(kafkaParams(GlobalContants.GROUP_ID), topicAndPartitions)
    if (errOrConsumeOffsets.isLeft) {
      throw new SparkException("尝试获取topic的消费偏移量失败....")
    }
    val consumeOffsetsMap: Map[TopicAndPartition, Long] = errOrConsumeOffsets.right.get
    consumeOffsetsMap
  }


  /**
    * 打印分区起始消费偏移量
    *
    * @param consumeOffsetMap
    */
  def printConsumeOffset(consumeOffsetMap: Map[TopicAndPartition, Long]) = {
    println("===========================================================")
    consumeOffsetMap.foreach(t2 => {
      println(s"【topic ${t2._1.topic}   partition:${t2._1.partition}    fromOffset:${t2._2}】")
    })
    println("===========================================================")
  }

  //支持高并发访问的容器
  private val offsetRangesBuffer = new AtomicReference[Array[OffsetRange]]()

  /**
    * 创建输入的dstream
    */
  def createDirectStream[
  K: ClassTag, //key的类型，key必须是一个类
  V: ClassTag, //value的类型
  KD <: Decoder[K] : ClassTag, //key的反序列化类
  VD <: Decoder[V] : ClassTag, //value的反序列化类
  R: ClassTag //返回值的类型
  ](ssc: StreamingContext) = {
    //获取起始消费偏移量
    val consumeOffsetMap = getConsumeOffset()
    //打印起始消费偏移量
    printConsumeOffset(consumeOffsetMap)
    //创建输入的dstream
    val kafkaInputDstream = KafkaUtils.createDirectStream[K, V, KD, VD, R](ssc, kafkaParams, consumeOffsetMap, (messageAndMataData: MessageAndMetadata[K, V]) => {
      messageAndMataData.message().asInstanceOf[R]
    }).transform(rdd => {
      val hasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges]
      val offsetRanges: Array[OffsetRange] = hasOffsetRanges.offsetRanges
      offsetRangesBuffer.set(offsetRanges)
      rdd
    })
    kafkaInputDstream
  }

  /**
    * 更新消费偏移量
    */
  def updateConsumeOffset() = {
    val offsetRanges: Array[OffsetRange] = offsetRangesBuffer.get()
    println("===========================================================")
    offsetRanges.foreach(offsetRange => {
      println(s"【topic:${offsetRange.topic}   partition:${offsetRange.partition}    offsetRange:${offsetRange.untilOffset}】")
      //开始更新
      val topicAndPartitionLong = Map[TopicAndPartition, Long](offsetRange.topicAndPartition() -> offsetRange.untilOffset)
      kafkaCluster.setConsumerOffsets(kafkaParams(GlobalContants.GROUP_ID), topicAndPartitionLong)
    })
    println("===========================================================")
  }


}
