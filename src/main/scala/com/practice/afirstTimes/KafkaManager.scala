package com.practice.afirstTimes

import com.daoke360.common.GlobalContants
import kafka.common.TopicAndPartition
import org.apache.spark.SparkException
import org.apache.spark.streaming.kafka.KafkaCluster
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import scala.collection.mutable

class KafkaManager(var kafkaParams: Map[String, String], var topicSet: Set[String]) {

  val kafkaCluster = new KafkaCluster(kafkaParams)

  def setOrUpdataAndPartition() = {
    var isConsume = true
    val errOrTopicAndPartitions = kafkaCluster.getPartitions(topicSet)
    if (errOrTopicAndPartitions.isLeft)
      throw new SparkException("尝试获取topic的元数据信息失败...")
    val topicAndPartitions = errOrTopicAndPartitions.right.get
    val errOrTopicAndPartition = kafkaCluster.getEarliestLeaderOffsets(topicAndPartitions)
    if (errOrTopicAndPartition.isLeft)
      throw new SparkException("尝试获取topic的最早一条消息的消费偏移量失败...")
    val errOrUpdateMap: Map[TopicAndPartition, LeaderOffset] = errOrTopicAndPartition.right.get

    val errOrPartititonToLong = kafkaCluster.getConsumerOffsets(kafkaParams(GlobalContants.GROUP_ID), topicAndPartitions)
    if (errOrPartititonToLong.isLeft)
      isConsume = false

    val setOrUpdateMap = mutable.Map[TopicAndPartition, Long]()

    if (isConsume) {
      val partitionToLong = errOrPartititonToLong.right.get
      partitionToLong.foreach(t2 => {
        val currentPartition = t2._1
        val currenOffset = t2._2
        val earliestLeaderOffset = errOrUpdateMap(currentPartition).offset
        if (currenOffset < earliestLeaderOffset) {
          setOrUpdateMap.put(currentPartition, earliestLeaderOffset)
        }
      })
    } else {
      errOrUpdateMap.foreach(t2 => {
        val earOrUpdatePartition = t2._1
        val earOrUpdateOffset = errOrUpdateMap(earOrUpdatePartition).offset
        setOrUpdateMap.put(earOrUpdatePartition,earOrUpdateOffset)
      })
    }
    kafkaCluster.setConsumerOffsets(kafkaParams(GlobalContants.GROUP_ID),setOrUpdateMap.toMap)
  }

}
