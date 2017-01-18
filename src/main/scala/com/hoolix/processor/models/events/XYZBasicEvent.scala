package com.hoolix.processor.models.events

import scala.collection.JavaConversions


/**
  * Created by peiyuchao on 2017/1/6.
  */
case class XYZBasicEvent(
                          token: String,
                          _type: String,
                          tags: Seq[String],
                          message: String,
                          uploadType: String,
                          uploadTimestamp: Long // when it is uploaded
 ) extends Event {

  override def toPayload = collection.mutable.Map(
    "token" -> token,
    "type" -> _type,
    "tags" -> JavaConversions.asJavaCollection(tags),
    "message" -> message,
    "upload_type" -> uploadType,
    "upload_timestamp" -> uploadTimestamp
  )
}
