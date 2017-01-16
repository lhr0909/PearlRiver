package com.hoolix.processor.decoders

import com.hoolix.processor.models.events.{Event, LineEvent, XYZBasicEvent}
import org.slf4j.LoggerFactory

case class RawLineDecoder(token: String, _type: String, tags: Seq[String], uploadType: String) extends Decoder {
  lazy val logger = LoggerFactory.getLogger(this.getClass)
  override def decode(event: Event): XYZBasicEvent = {
    val payload = event.asInstanceOf[LineEvent].toPayload
    XYZBasicEvent(
      token,
      _type,
      tags,
      payload("message").asInstanceOf[String],
      uploadType,
      System.currentTimeMillis
    )
  }
}
