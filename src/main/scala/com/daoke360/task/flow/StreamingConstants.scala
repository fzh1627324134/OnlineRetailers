package com.daoke360.task.flow

import com.daoke360.common.GlobalContants
import com.daoke360.conf.ConfigurationManager

object StreamingConstants {

  //kafka集群节点列表
  val metaDataAndBrokerList = ConfigurationManager.getProperty(GlobalContants.METADATA_BROKER_LIST)
  //kafka topic
  val kafkaTopic = ConfigurationManager.getProperty(GlobalContants.KAFKA_TOPIC)
  //消费位置
  val autoOffsetReset = ConfigurationManager.getProperty(GlobalContants.AUTO_OFFSET_RESET)
  //消费组id
  val groupId = ConfigurationManager.getProperty(GlobalContants.GROUP_ID)
  //checkpoint path
  val checkpointPath = ConfigurationManager.getProperty(GlobalContants.STREAMING_CHECKPOINT_PATH)
  //batch产生的时间间隔
  val batchInterval = ConfigurationManager.getProperty(GlobalContants.BATCH_INTERVAL).toInt

  val kafkaParams = Map[String, String](
    GlobalContants.METADATA_BROKER_LIST -> metaDataAndBrokerList,
    GlobalContants.GROUP_ID -> groupId,
    GlobalContants.AUTO_OFFSET_RESET -> autoOffsetReset
  )

}
