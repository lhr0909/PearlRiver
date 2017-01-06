package com.hoolix.processor.decoders

import com.hoolix.processor.models.{Event, FileBeatEvent, XYZBasicEvent}
import org.slf4j.LoggerFactory

/**
  * Hoolix 2016
  * Created by simon on 12/8/16.
  */
case class FileBeatDecoder() extends Decoder {
  lazy val logger = LoggerFactory.getLogger(this.getClass)
  override def decode(event: Event): XYZBasicEvent = {
    println(event)
    val payload = event.asInstanceOf[FileBeatEvent].toPayload
    println(payload)
    XYZBasicEvent(
      payload.get("fields").asInstanceOf[Map[String, String]].get("token").asInstanceOf[String],
      payload.get("type").asInstanceOf[String],
      payload.get("tags").asInstanceOf[Seq[String]],
      payload.get("message").asInstanceOf[String],
      "streaming",
      payload.get("timestamp").asInstanceOf[Long]
    )
  }
}