package com.hoolix.processor.sources

import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.Source
import com.hoolix.processor.models.{Event, FileBeatEvent, KafkaTransmitted, LineEvent}

import scala.concurrent.Future

/**
  * Hoolix 2017
  * Created by simon on 1/1/17.
  */
object KafkaSource {

  def convertToEvent(committableMessage: CommittableMessage[Array[Byte], String]): Future[KafkaTransmitted] = {
//    val Event = new EventBuilder()
//      .setCommittableOffset(committableMessage.committableOffset)
//      .setEvent(FileBeatEvent.fromJsonString(committableMessage.record.value))
//      .build()
//    Event()
    println(committableMessage)
    val event = KafkaTransmitted(committableMessage.committableOffset, FileBeatEvent.fromJsonString(committableMessage.record.value))
    println(event)
//    val event = KafkaTransmitted(committableMessage.committableOffset, LineEvent(committableMessage.record.value))

    //    val event = new FileBeatEvent(committableMessage.committableOffset, committableMessage.record.value)
    Future.successful(event)
  }

  def apply(
             parallelism: Int,
             kafkaConsumerSettings: ConsumerSettings[Array[Byte], String],
             kafkaTopics: Set[String]
           ): Source[KafkaTransmitted, Consumer.Control] = {
    Consumer.committableSource(kafkaConsumerSettings, Subscriptions.topics(kafkaTopics))
      .mapAsync(parallelism)(KafkaSource.convertToEvent)
  }
}
