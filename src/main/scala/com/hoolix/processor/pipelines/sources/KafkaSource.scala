package com.hoolix.processor.pipelines.sources

import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.Consumer
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Source
import com.hoolix.processor.models.builders.KafkaEventBuilder
import com.hoolix.processor.models.{FilebeatEvent, KafkaEvent}

import scala.concurrent.Future

/**
  * Hoolix 2017
  * Created by simon on 1/1/17.
  */
case class KafkaSource(
                      parallelism: Int,
                      kafkaConsumerSettings: ConsumerSettings[Array[Byte], String],
                      kafkaTopics: String*
                      ) {

  def toSource: Source[KafkaEvent, Consumer.Control] = {
    Consumer.committableSource(kafkaConsumerSettings, Subscriptions.topics(kafkaTopics.toSet))
      .mapAsync(parallelism) { committableMessage: CommittableMessage[Array[Byte], String] =>
        val kafkaEvent = new KafkaEventBuilder()
          .setCommittableOffset(committableMessage.committableOffset)
          .setEvent(FilebeatEvent.fromJsonString(committableMessage.record.value))
          .build()
        Future.successful(kafkaEvent)
      }
  }

}
