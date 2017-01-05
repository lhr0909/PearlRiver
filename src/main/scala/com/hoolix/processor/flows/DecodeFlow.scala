package com.hoolix.processor.flows

import akka.NotUsed
import com.hoolix.processor.decoders.Decoder
import akka.stream.scaladsl.Flow
import com.hoolix.processor.models.Event

import scala.concurrent.Future

/**
  * Created by peiyuchao on 2017/1/4.
  */
case class DecodeFlow(parallelism: Int, decoder: Decoder) {
  def toFlow: Flow[Event, Event, NotUsed] = {
    Flow[Event].mapAsync(parallelism)((event: Event) => {
      Future.successful(decoder.decode(event))
    })
  }
}
