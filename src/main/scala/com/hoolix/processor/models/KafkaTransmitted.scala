package com.hoolix.processor.models

import akka.kafka.ConsumerMessage.CommittableOffset
import com.hoolix.processor.models.events.Event
import org.elasticsearch.action.index.IndexRequest

/**
  * Created by peiyuchao on 2017/1/5.
  */
case class KafkaTransmitted(offset: CommittableOffset, event: Event) {
  def topic: String = offset.partitionOffset.key.topic
  def partition: Int = offset.partitionOffset.key.partition
  def partitionOffset: Long = offset.partitionOffset.offset

  def indexName: String = event.indexName
  def indexType: String = event.indexType
  def docId: String = s"$topic.$partition.$partitionOffset"
}
