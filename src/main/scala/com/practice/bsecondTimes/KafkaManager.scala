package com.practice.bsecondTimes

import com.daoke360.common.GlobalContants
import kafka.common.TopicAndPartition
import org.apache.spark.SparkException
import org.apache.spark.streaming.kafka.KafkaCluster
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import scala.collection.mutable

class KafkaManager(var kafkaParams: Map[String, String], var topicSet: Set[String]) {

  val kafkaCluster = new KafkaCluster(kafkaParams)

  def setOrUpdateConsumeOffset() = {
    var isConsume = true

    val errOrTopAndPartitions = kafkaCluster.getPartitions(topicSet)
    if (errOrTopAndPartitions.isLeft)
      throw new SparkException("尝试获取topic元数据信息失败...")
    val topicAndPartitions: Set[TopicAndPartition] = errOrTopAndPartitions.right.get

    val errOrearliestLeaderOffsets = kafkaCluster.getEarliestLeaderOffsets(topicAndPartitions)
    if (errOrearliestLeaderOffsets.isLeft)
      throw new SparkException("尝试获取topic的最早一条消费偏移量失败...")

    val earliesLeaderOffsetMap: Map[TopicAndPartition, LeaderOffset] = errOrearliestLeaderOffsets.right.get

    val errOrPartitionToLong = kafkaCluster.getConsumerOffsets(kafkaParams(GlobalContants.GROUP_ID), topicAndPartitions)
    if (errOrPartitionToLong.isLeft)
      isConsume = false

    val setOrUpdateMap = mutable.Map[TopicAndPartition, Long]()

    if (isConsume) {
      val consumeOffsetsMap = errOrPartitionToLong.right.get
      consumeOffsetsMap.foreach(t2 => {
        val currentPartition = t2._1
        val currentOffset = t2._2
        val earliestLeaderOffset = earliesLeaderOffsetMap(currentPartition).offset
        if (currentOffset < earliestLeaderOffset){
          setOrUpdateMap.put(currentPartition,earliestLeaderOffset)
        }
      })
    }else{
      earliesLeaderOffsetMap.foreach(t2=>{
        val earliestLeaderPartition = t2._1
        val earliestLeaderOffset = earliesLeaderOffsetMap(earliestLeaderPartition).offset
        setOrUpdateMap.put(earliestLeaderPartition,earliestLeaderOffset)
      })
    }
    kafkaCluster.setConsumerOffsets(kafkaParams(GlobalContants.GROUP_ID),setOrUpdateMap.toMap)
  }

}
