package com.hoolix.processor.models

import org.elasticsearch.action.index.IndexRequest

/**
  * Created by peiyuchao on 2017/1/5.
  */
case class IntermediateEvent(
                              payload: collection.mutable.Map[String, Any]
                            ) extends Event {

  override def toPayload: collection.mutable.Map[String, Any] = payload

  override def indexName: String = {
    val token = payload.get("token") match {
      case Some(x: String) => x
      case _ => "_unknown_"
    }
//    val `type` = getType
//    val uploadTimestamp = payload.get("upload_timestamp").asInstanceOf[Some[Long]].get
//    val eventTimestamp = payload.getOrDefault("event_timestamp", null).asInstanceOf[Long]
    // TODO 日期轮转
//    if (eventTimestamp != null) token + Event.INDEX_NAME_SEPARATOR + `type` + eventTimestamp + uploadTimestamp
//    else token + Event.INDEX_NAME_SEPARATOR + `type` + Event.INDEX_NAME_WILDCARD + uploadTimestamp
    token + "." + indexType
  }

  override def indexType: String = payload.get("type") match {
    case Some(x: String) => x
    case _ => "_"
  }

  override def docId = ???
  override def toIndexRequest = new IndexRequest(indexName, indexType) // let ES generate a UUID for this
}
