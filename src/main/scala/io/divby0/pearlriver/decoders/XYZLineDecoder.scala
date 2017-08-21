package io.divby0.pearlriver.decoders

import io.divby0.pearlriver.models.{Event, XYZBasicEvent, LineEvent}
import org.slf4j.LoggerFactory

/**
  * token type tags streaming/file || message
  */
case class XYZLineDecoder() extends Decoder {
  lazy val logger = LoggerFactory.getLogger(this.getClass)
  override def decode(event: Event): XYZBasicEvent = {
    val payload = event.asInstanceOf[LineEvent].toPayload
    val message: String = payload("message").toString
    val segments = message.split(" \\|\\| ", 2)
    if (segments.size == 2) {
      val headers = segments(0).split(" ", 4)
      val body = segments(1)
      if (headers.size == 4) {
        XYZBasicEvent(
          headers(0),
          headers(1),
          headers(2).split(","),
          body,
          headers(3),
          System.currentTimeMillis
        )
      } else {
        throw new IllegalArgumentException()
      }
    } else {
      throw new IllegalArgumentException()
    }
  }
}
