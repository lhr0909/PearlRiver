package com.hoolix.processor.decoders

import com.hoolix.processor.models.{Event, LineEvent, XYZBasicEvent}
import org.slf4j.LoggerFactory

case class RawLineDecoder(token: String, _type: String, tags: Seq[String], uploadType: String) extends Decoder {
  lazy val logger = LoggerFactory.getLogger(this.getClass)
  override def decode(event: Event): XYZBasicEvent = {
    println("enter RawLineDecoder")
    val payload = event.asInstanceOf[LineEvent].toPayload
    println(event)
    println(payload)

    println(token)
    println(_type)
    println(tags)
    println(uploadType)
    println(payload("message").asInstanceOf[String])
    val result = XYZBasicEvent(
      token,
      _type,
      tags,
      payload("message").asInstanceOf[String],
      uploadType,
      System.currentTimeMillis
    )
    println("leave RawLineDecoder")
    result
  }
}
