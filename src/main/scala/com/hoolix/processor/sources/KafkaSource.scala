package com.hoolix.processor.sources

import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.Source
import com.hoolix.processor.models.{FileBeatEvent, Event}

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

  def convertToEvent(committableMessage: CommittableMessage[Array[Byte], String]): Future[Event] = {
//    val Event = new EventBuilder()
//      .setCommittableOffset(committableMessage.committableOffset)
//      .setEvent(FileBeatEvent.fromJsonString(committableMessage.record.value))
//      .build()
//    Event()
    val event = new FileBeatEvent(committableMessage.committableOffset, committableMessage.record.value)
    Future.successful(event)
  }

  def toSource: Source[Event, Consumer.Control] = {
    Consumer.committableSource(kafkaConsumerSettings, Subscriptions.topics(kafkaTopics.toSet))
      .mapAsync(parallelism)(convertToEvent)
  }

}
