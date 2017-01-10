package com.hoolix.processor.sources

import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.Subscriptions
import akka.kafka.scaladsl.Consumer
import akka.stream.scaladsl.Source
import com.hoolix.processor.models.{FileBeatEvent, KafkaTransmitted}
import com.hoolix.processor.modules.KafkaConsumerSettings
import com.typesafe.config.Config

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
//    println(committableMessage)
    val event = KafkaTransmitted(committableMessage.committableOffset, FileBeatEvent.fromJsonString(committableMessage.record.value))
//    println(event)
//    val event = KafkaTransmitted(committableMessage.committableOffset, LineEvent(committableMessage.record.value))

    //    val event = new FileBeatEvent(committableMessage.committableOffset, committableMessage.record.value)
    Future.successful(event)
  }

  def apply(
             parallelism: Int,
             kafkaTopic: String
           )(implicit config: Config, system: ActorSystem): Source[KafkaTransmitted, Consumer.Control] = {

    //FIXME: rolling back to single topic per stream for now, need more fine-grained control
    Consumer.committableSource(KafkaConsumerSettings(), Subscriptions.topics(Set(kafkaTopic)))
      .mapAsync(parallelism)(KafkaSource.convertToEvent)

  }
}


