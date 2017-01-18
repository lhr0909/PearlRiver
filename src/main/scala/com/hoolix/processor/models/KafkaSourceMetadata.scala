package com.hoolix.processor.models

import org.apache.kafka.clients.consumer.ConsumerRecord

/**
  * Hoolix 2017
  * Created by simon on 1/14/17.
  */
case class KafkaSourceMetadata(consumerRecord: ConsumerRecord[String, String]) extends SourceMetadata {

  type OffsetT = ConsumerRecord[String, String]

  override val offset: OffsetT = consumerRecord

  def topic: String = offset.topic()
  def partition: Int = offset.partition()
  def partitionOffset: Long = offset.offset()

  override def id: String = s"$topic.$partition.$partitionOffset"

}
