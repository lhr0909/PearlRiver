package com.hoolix.processor.sources

import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.Source
import com.hoolix.processor.models.builders.KafkaEventBuilder
import com.hoolix.processor.models.{FilebeatEvent, KafkaEvent}
import com.hoolix.processor.modules.KafkaConsumerSettings
import com.typesafe.config.Config

import scala.concurrent.Future

/**
  * Hoolix 2017
  * Created by simon on 1/1/17.
  */
object KafkaSource {

  def convertToKafkaEvent(committableMessage: CommittableMessage[Array[Byte], String]): Future[KafkaEvent] = {
    val kafkaEvent = new KafkaEventBuilder()
      .setCommittableOffset(committableMessage.committableOffset)
      .setEvent(FilebeatEvent.fromJsonString(committableMessage.record.value))
      .build()
    Future.successful(kafkaEvent)
  }

  def apply(
             parallelism: Int,
             kafkaTopics: Set[String]
           )(implicit config: Config, system: ActorSystem): Source[KafkaEvent, Consumer.Control] = {

    Consumer.committableSource(KafkaConsumerSettings(), Subscriptions.topics(kafkaTopics))
      .mapAsync(parallelism)(KafkaSource.convertToKafkaEvent)

  }
}


