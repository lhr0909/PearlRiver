package com.hoolix.processor.flows

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.hoolix.processor.decoders.Decoder
import com.hoolix.processor.models.events.IntermediatePreviewEvent
import com.hoolix.processor.models.{PortFactory, Shipper, SourceMetadata}
import scala.concurrent.Future

/**
  * Created by peiyuchao on 2017/1/13.
  */
case class DecodePreviewFlow[SrcMeta <: SourceMetadata, PortFac <: PortFactory](parallelism: Int, decoder: Decoder) {

  type Shipperz = Shipper[SrcMeta, PortFac]

  def flow: Flow[Shipperz, Shipperz, NotUsed] = {
    Flow[Shipperz].mapAsync(parallelism)((shipper: Shipperz) => {
      val decoded = decoder.decode(shipper.event)
      val initialHighlights: Map[String, Any] = decoded.toPayload.map((key) => key._1 -> (-1, -1)).toMap[String, Any]
      val preview = IntermediatePreviewEvent(collection.mutable.Map(initialHighlights.toSeq: _*), decoded.toPayload)
      Future.successful(
        Shipper(
          preview,
          shipper.sourceMetadata,
          shipper.portFactory
        )
      )
    }).named("decode-preview-flow")
  }
}
