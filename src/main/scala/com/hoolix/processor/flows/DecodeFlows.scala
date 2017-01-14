package com.hoolix.processor.flows

import akka.NotUsed
import com.hoolix.processor.decoders.Decoder
import akka.stream.scaladsl.Flow
import com.hoolix.processor.models.{ByteStringTransmitted, KafkaTransmitted}

import scala.concurrent.Future

/**
  * Created by peiyuchao on 2017/1/4.
  */
object DecodeFlows {
  def kafkaDecodeFlow(parallelism: Int, decoder: Decoder): Flow[KafkaTransmitted, KafkaTransmitted, NotUsed] = {
    Flow[KafkaTransmitted].mapAsync(parallelism)((kafkaTransmitted: KafkaTransmitted) => {
      Future.successful(KafkaTransmitted(kafkaTransmitted.offset, decoder.decode(kafkaTransmitted.event)))
    }).named("kafka-decode-flow")
  }

  def byteStringDecodeFlow(parallelism: Int, decoder: Decoder): Flow[ByteStringTransmitted, ByteStringTransmitted, NotUsed] = {
    Flow[ByteStringTransmitted].mapAsync(parallelism)((byteStringTransmitted: ByteStringTransmitted) => {
      Future.successful(ByteStringTransmitted(byteStringTransmitted.offset, decoder.decode(byteStringTransmitted.event)))
    }).named("bytestring-decode-flow")
  }
}
