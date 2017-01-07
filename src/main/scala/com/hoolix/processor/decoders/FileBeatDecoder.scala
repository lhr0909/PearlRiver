package com.hoolix.processor.decoders

import com.hoolix.processor.models.{Event, FileBeatEvent, XYZBasicEvent}
import org.slf4j.LoggerFactory

/**
  * Hoolix 2016
  * Created by simon on 12/8/16.
  */
case class  FileBeatDecoder() extends Decoder {
  lazy val logger = LoggerFactory.getLogger(this.getClass)
  override def decode(event: Event): XYZBasicEvent = {
    val payload = event.asInstanceOf[FileBeatEvent].toPayload
    val token = payload.get("fields") match {
      case Some(map) => map.asInstanceOf[Map[String, String]].get("token").toString
    }
    val tags = payload.get("tags") match {
      case Some(seq) => seq.asInstanceOf[Seq[String]]
    }
    val timestamp = payload.get("timestamp") match {
      case Some(long) => long.asInstanceOf[Long]
    }

    XYZBasicEvent(
      token,
      payload.get("type").asInstanceOf[Some[String]].get,
      tags,
      payload.get("message").asInstanceOf[Some[String]].get,
      "streaming",
      timestamp
    )
  }
}
