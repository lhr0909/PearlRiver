package com.hoolix.processor.decoders

import java.util.Date

import com.hoolix.processor.models.events.{Event, FileBeatEvent, XYZBasicEvent}
import org.slf4j.LoggerFactory

/**
  * Hoolix 2016
  * Created by simon on 12/8/16.
  */
case class  FileBeatDecoder() extends Decoder {
  lazy val logger = LoggerFactory.getLogger(this.getClass)
  override def decode(event: Event): XYZBasicEvent = {
    val payload = event.asInstanceOf[FileBeatEvent].toPayload

    //TODO: decide what to return when types don't match (should we really return _unknown_ ?

    val token = payload.get("fields") match {
      case Some(map) =>
        map.asInstanceOf[Map[String, String]].get("token") match {
          case Some(x: String) => x
          case _ => "_unknown_"
        }
      case _ => "_unknown_"
    }

    val tags = payload.get("tags") match {
      case Some(seq) => seq.asInstanceOf[Seq[String]]
      case _ => Seq[String]()
    }

    val timestamp = payload.get("timestamp") match {
      case Some(timestamp: Long) => timestamp
      case _ => new Date().getTime
    }

    val typ = payload.get("type") match {
      case Some(t: String) => t
      case _ => "_unknown_"
    }

    val msg = payload.get("message") match {
      case Some(m: String) => m
      case _ => "_"
    }

    XYZBasicEvent(
      token = token,
      _type = typ,
      tags = tags,
      message = msg,
      uploadType = "streaming",
      uploadTimestamp = timestamp
    )
  }
}
