package com.hoolix.processor.sources

import java.nio.charset.Charset

import akka.stream.scaladsl.{Framing, Source}
import akka.util.ByteString
import com.hoolix.processor.models.{ByteStringTransmitted, FileOffset, LineEvent}

import scala.concurrent.Future

/**
  * Hoolix 2017
  *
  * This source handles incoming [[akka.util.ByteString]] streams via Akka HTTP and forms each line
  * into a [[com.hoolix.processor.models.LineEvent]]
  *
  * Created by simon on 1/13/17.
  */
object ByteStringSource {
  val maxBytesFrame: Int = 10 * 1024

  def convertToEvent(line: ByteString, fileName: String, offset: Long): ByteStringTransmitted = {
    val lineString = line.decodeString(Charset.defaultCharset())
    val lineEvent = LineEvent(lineString)
    ByteStringTransmitted(FileOffset(fileName, offset), lineEvent)
  }

  def apply(
             parallelism: Int,
             fileName: String,
             incomingSource: Source[ByteString, Any]
           ): Source[ByteStringTransmitted, Any] = {
    incomingSource
      .via(Framing.delimiter(ByteString("\n"), maxBytesFrame)).zipWithIndex
      .mapAsync(parallelism) { zipped: (ByteString, Long) =>
        val (line, offset) = zipped
        Future.successful(convertToEvent(line, fileName, offset))
      }.named("bytestring-source")
  }
}
