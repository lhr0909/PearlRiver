package com.hoolix.processor.models.events

import org.elasticsearch.action.index.IndexRequest

/**
  * Created by peiyuchao on 2017/1/12.
  */
case class IntermediatePreviewEvent(
  highlights: collection.mutable.Map[String, Any],
  payload: collection.mutable.Map[String, Any]
) extends Event {
  override def toPayload: collection.mutable.Map[String, Any] = payload
  def docId = ???
  override def indexName = ???
  override def indexType = ???
  override def toIndexRequest: IndexRequest = ???
}
