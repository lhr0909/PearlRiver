package com.hoolix.pipeline.util

import com.hoolix.pipeline.core.Config
import kafka.common.TopicAndPartition

class KafkaUtils(param:Map[String, String], topic_and_partition:Seq[(String,Int)]) {
  val cluster = new KafkaCluster(param)

  val topic_and_partition_set = topic_and_partition.map { case (topic, partition) =>
    new TopicAndPartition(topic, partition)
  }.toSet

  lazy val earliest_offset = cluster.getEarliestLeaderOffsets(topic_and_partition_set).right.getOrElse(Map())
  lazy val latest_offset   = cluster.getLatestLeaderOffsets(topic_and_partition_set).right.getOrElse(Map())


  def get_earliest_offset(tp: TopicAndPartition, default:Long = -1) = {
    earliest_offset.getOrElse(tp, null) match {
      case null   => default
      case offset => offset.offset
    }
  }

  def get_latest_offset(tp: TopicAndPartition, default:Long = -1) = {
    latest_offset.getOrElse(tp, null) match {
      case null   => default
      case offset => offset.offset
    }
  }

  def get_offset_range() = {
    topic_and_partition_set.map { tp =>
      (tp.topic, (tp.partition)) -> (get_earliest_offset(tp, -1), get_latest_offset(tp, -1))
    }
  }
}

object KafkaUtils {
  def apply(config:Config): KafkaUtils = {
    val kafka_config = Utils.filterConf(config.origin, "spark.input.kafka.")
    new KafkaUtils(kafka_config, config.spark_input_kafka_topic_and_partition)
  }

}
