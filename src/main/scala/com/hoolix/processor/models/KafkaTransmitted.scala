package com.hoolix.processor.models

import akka.kafka.ConsumerMessage.CommittableOffset
import org.elasticsearch.action.index.IndexRequest

/**
  * Created by peiyuchao on 2017/1/5.
  */
case class KafkaTransmitted(committableOffset: CommittableOffset, event: Event) extends ElasticSearchSinkAble {
  def topic: String = committableOffset.partitionOffset.key.topic
  def partition: Int = committableOffset.partitionOffset.key.partition
  def offset: Long = committableOffset.partitionOffset.offset

  override def indexName: String = event.indexName
  override def indexType: String = event.indexType
  override def docId: String = topic + "." + partition + "." + offset
  override def toIndexRequest: IndexRequest = new IndexRequest(indexName, indexType, docId)
}
