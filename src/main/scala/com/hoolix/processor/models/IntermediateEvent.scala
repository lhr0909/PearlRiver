package com.hoolix.processor.models

import org.elasticsearch.action.index.IndexRequest

/**
  * Created by peiyuchao on 2017/1/5.
  */
case class IntermediateEvent(payload: collection.mutable.Map[String, Any]) extends Event {
  override def toPayload = payload

  override def indexName = ???
//  {
//    val token = payload.get("token").toString
//    val `type` = getType
//    val uploadTimestamp = payload.get("upload_timestamp").asInstanceOf[Long]
//    val eventTimestamp = payload.getOrDefault("event_timestamp", null).asInstanceOf[Long]
//    // TODO 日期轮转
//    if (eventTimestamp != null) token + Event.INDEX_NAME_SEPARATOR + `type` + eventTimestamp + uploadTimestamp
//    else token + Event.INDEX_NAME_SEPARATOR + `type` + Event.INDEX_NAME_WILDCARD + uploadTimestamp
//  }
  override def indexType = payload.get("type").asInstanceOf[String]
  override def docId = ???
  override def toIndexRequest = new IndexRequest(indexName, indexType, docId)
}
