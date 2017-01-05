package com.hoolix.processor.decoders

import com.hoolix.processor.models.{Event, RawLineEvent, XYZBasicEvent}
import org.slf4j.LoggerFactory

case class RawLineDecoder(token: String, _type: String, tags: java.util.List[String], uploadType: String) extends Decoder {
  lazy val logger = LoggerFactory.getLogger(this.getClass)
  override def decode(event: Event): XYZBasicEvent = {
    val payload = event.asInstanceOf[RawLineEvent].toPayload
    new XYZBasicEvent(
      token,
      _type,
      tags,
      payload.get("message").asInstanceOf[String],
      uploadType,
      System.currentTimeMillis
    )
  }
}
