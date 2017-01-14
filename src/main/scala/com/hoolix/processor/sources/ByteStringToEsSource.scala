package com.hoolix.processor.sources

import java.nio.charset.Charset

import akka.stream.scaladsl.{Framing, Source}
import akka.util.ByteString
import com.hoolix.processor.models.events.LineEvent
import com.hoolix.processor.models._

import scala.concurrent.Future

/**
  * Hoolix 2017
  *
  * This source handles incoming [[akka.util.ByteString]] streams via Akka HTTP and forms each line
  * into a [[LineEvent]]
  *
  * Created by simon on 1/13/17.
  */
case class ByteStringToEsSource(
                           parallel: Int,
                           fileName: String,
                           incomingSource: Source[ByteString, Any]
                           ) extends AbstractSource {
  override type SrcMeta = FileSourceMetadata
  override type PortFac = ElasticsearchPortFactory

  override type S = (ByteString, Long)
  override type Mat = Any

  override val sourceType: String = "byte-string"

  override val parallelism: Int = parallel

  val maxBytesFrame: Int = 10 * 1024

  override val startingSource: Source[S, Any] =
    incomingSource
      .via(Framing.delimiter(ByteString("\n"), maxBytesFrame)).zipWithIndex


  override def convertToShipper(incoming: S): Future[Shipper[FileSourceMetadata, ElasticsearchPortFactory]] = {
    val (line, offset) = incoming
    val lineString = line.decodeString(Charset.defaultCharset())
    val lineEvent = LineEvent(lineString)
    Future.successful(
      Shipper(
        lineEvent,
        FileSourceMetadata(FileOffset(fileName, offset)),
        ElasticsearchPortFactory()
      )
    )
  }
}
