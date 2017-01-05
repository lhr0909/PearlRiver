package com.hoolix.processor.decoders

import com.hoolix.processor.models.{Event, XYZBasicEvent, XYZLineEvent}
import org.slf4j.LoggerFactory

/**
  * token type tags streaming/file || message
  */
case class XYZLineDecoder() extends Decoder {
  lazy val logger = LoggerFactory.getLogger(this.getClass)
  override def decode(event: Event): XYZBasicEvent = {
    val payload = event.asInstanceOf[XYZLineEvent].toPayload
    val message = payload.get("message").asInstanceOf[String]
    val segments = message.split(" \\|\\| ", 2)
    if (segments.size != 2) {
      // TODO
    } else {
      val headers = segments(0).split(" ", 4)
      val body = segments(1)
      if (headers.size != 4) {
        // TODO
      } else {
        new XYZBasicEvent(
          headers(0),
          headers(1),
          headers(2).split(",").asInstanceOf[java.util.List[String]],
          body,
          headers(3),
          System.currentTimeMillis
        )
      }
    }
  }
}
