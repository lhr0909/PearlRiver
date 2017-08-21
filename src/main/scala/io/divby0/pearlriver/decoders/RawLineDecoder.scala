package io.divby0.pearlriver.decoders

import io.divby0.pearlriver.models.{Event, LineEvent, XYZBasicEvent}
import org.slf4j.LoggerFactory

case class RawLineDecoder(token: String, _type: String, tags: Seq[String], uploadType: String) extends Decoder {
  lazy val logger = LoggerFactory.getLogger(this.getClass)
  override def decode(event: Event): XYZBasicEvent = {
    val payload = event.asInstanceOf[LineEvent].toPayload
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
