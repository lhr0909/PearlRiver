package com.hoolix.processor.models

import org.elasticsearch.action.index.IndexRequest

import scala.collection.JavaConversions


/**
  * Created by peiyuchao on 2017/1/6.
  */
case class XYZBasicEvent(
  token: String,
  `type`: String,
  tags: Seq[String],
  message: String,
  uploadType: String,
  uploadTimestamp: Long // when it is uploaded
) extends Event {

  override def toPayload = collection.mutable.Map(
    "token" -> token,
    "type" -> `type`,
    "tags" -> JavaConversions.asJavaCollection(tags),
    "message" -> message,
    "upload_type" -> uploadType,
    "upload_timestamp" -> uploadTimestamp
  )

  override def indexName = token + "." + `type`
//  getToken + Event.INDEX_NAME_SEPARATOR + getType
  override def indexType = `type`
  override def docId = ???
  override def toIndexRequest = new IndexRequest(indexName, indexType) // let ES create a UUID
}
