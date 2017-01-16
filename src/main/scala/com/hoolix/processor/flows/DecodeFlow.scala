package com.hoolix.processor.flows

import akka.NotUsed
import com.hoolix.processor.decoders.Decoder
import akka.stream.scaladsl.Flow
import com.hoolix.processor.models._

import scala.concurrent.Future

/**
  * Created by peiyuchao on 2017/1/4.
  */
case class DecodeFlow[SrcMeta <: SourceMetadata, PortFac <: PortFactory](parallelism: Int, decoder: Decoder) {

  type Shipperz = Shipper[SrcMeta, PortFac]

  def flow: Flow[Shipperz, Shipperz, NotUsed] = {
    Flow[Shipperz].mapAsync(parallelism)((shipper: Shipperz) => {
      Future.successful(
        Shipper(
          decoder.decode(shipper.event),
          shipper.sourceMetadata,
          shipper.portFactory
        )
      )
    }).named("decode-flow")
  }
}
