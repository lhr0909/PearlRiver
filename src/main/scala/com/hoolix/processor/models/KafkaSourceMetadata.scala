package com.hoolix.processor.models

import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition

/**
  * Hoolix 2017
  * Created by simon on 1/14/17.
  */
case class KafkaSourceMetadata(consumerRecord: ConsumerRecord[String, String]) extends SourceMetadata {

  type OffsetT = ConsumerRecord[String, String]

  override val offset: OffsetT = consumerRecord

  lazy val topicPartition: TopicPartition = new TopicPartition(offset.topic(), offset.partition())
  def offsetAndMetadata(metadata: String): OffsetAndMetadata = new OffsetAndMetadata(offset.offset(), metadata)

  override def id: String = s"${offset.topic()}.${offset.partition()}.${offset.offset()}"

}
