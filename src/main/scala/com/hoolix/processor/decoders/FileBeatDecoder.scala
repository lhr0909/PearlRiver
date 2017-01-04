package com.hoolix.processor.decoders

import com.hoolix.processor.models.{Event, FileBeatEvent, XYZBasicEvent}

/**
  * Hoolix 2016
  * Created by simon on 12/8/16.
  */
case class FileBeatDecoder() extends Decoder {
  override def decode(event: Event): XYZBasicEvent = {
    val payload = event.asInstanceOf[FileBeatEvent].toPayload
    new XYZBasicEvent(
      payload.get("fields").asInstanceOf[Map[String, String]].get("token").asInstanceOf[String],
      payload.get("type").asInstanceOf[String],
      payload.get("tags").asInstanceOf[java.util.List[String]],
      payload.get("message").asInstanceOf[String],
      "streaming",
      payload.get("timestamp").asInstanceOf[Long]
    )
  }
}
