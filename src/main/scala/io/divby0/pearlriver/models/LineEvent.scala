package io.divby0.pearlriver.models

import org.elasticsearch.action.index.IndexRequest

/**
  * Created by peiyuchao on 2017/1/6.
  */
case class LineEvent(message: String) extends Event {
  override def toPayload = collection.mutable.Map("message" -> message)

  override def indexName = ???
  override def indexType = ???
  override def docId = ???
  override def toIndexRequest = new IndexRequest(indexName, indexType, docId)
}
