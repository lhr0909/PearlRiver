package com.hoolix.processor.models

import akka.kafka.ConsumerMessage.CommittableOffset

/**
  * Hoolix 2017
  * Created by simon on 1/14/17.
  */
case class KafkaSourceMetadata(committableOffset: CommittableOffset) extends SourceMetadata {

  type OffsetT = CommittableOffset

  override val offset: OffsetT = committableOffset

  def topic: String = offset.partitionOffset.key.topic
  def partition: Int = offset.partitionOffset.key.partition
  def partitionOffset: Long = offset.partitionOffset.offset

  override def id: String = s"$topic.$partition.$partitionOffset"

}
