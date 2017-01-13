package com.hoolix.processor.flows

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.hoolix.processor.decoders.Decoder
import com.hoolix.processor.models.{Event, IntermediatePreviewEvent}

import scala.concurrent.Future

/**
  * Created by peiyuchao on 2017/1/13.
  */
object DecodePreviewFlow {
  def apply(parallelism: Int, decoder: Decoder): Flow[Event, Event, NotUsed] = {
    println("Enter DecodePreviewFlow")
    Flow[Event].mapAsync(parallelism)((event: Event) => {
      val decoded = decoder.decode(event)
      println(decoded.toPayload.map((key) => key._1 -> (-1, -1)).toMap)
      val initialHighlights: Map[String, Any] = decoded.toPayload.map((key) => key._1 -> (-1, -1)).toMap[String, Any]
      val preview = IntermediatePreviewEvent(collection.mutable.Map(initialHighlights.toSeq: _*), decoded.toPayload)
      Future.successful(preview)
//      Future.successful(event)
    })
  }
}
