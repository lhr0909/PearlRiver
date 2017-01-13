package com.hoolix.processor.flows

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.hoolix.processor.decoders.Decoder
import com.hoolix.processor.models.Event
import scala.concurrent.Future

/**
  * Created by peiyuchao on 2017/1/13.
  */
object DecodePreviewFlow {
  def apply(parallelism: Int, decoder: Decoder): Flow[Event, Event, NotUsed] = {
    Flow[Event].mapAsync(parallelism)((event: Event) => {
      Future.successful(decoder.decode(event))
    })
  }
}
