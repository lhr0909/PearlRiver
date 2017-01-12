package com.hoolix.processor.flows

import akka.NotUsed
import com.hoolix.processor.decoders.Decoder
import akka.stream.scaladsl.Flow
import com.hoolix.processor.models.{Event, KafkaTransmitted}

import scala.concurrent.Future

/**
  * Created by peiyuchao on 2017/1/4.
  */
object DecodeFlow {
  def apply(parallelism: Int, decoder: Decoder): Flow[KafkaTransmitted, KafkaTransmitted, NotUsed] = {
    Flow[KafkaTransmitted].mapAsync(parallelism)((kafkaTransmitted: KafkaTransmitted) => {
      Future.successful(KafkaTransmitted(kafkaTransmitted.committableOffset, decoder.decode(kafkaTransmitted.event)))
    }).named("decode-flow")
  }
}
