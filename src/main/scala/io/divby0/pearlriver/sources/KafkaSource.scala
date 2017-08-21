package io.divby0.pearlriver.sources

import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.Subscriptions
import akka.kafka.scaladsl.Consumer
import akka.stream.scaladsl.Source
import io.divby0.pearlriver.models.{FileBeatEvent, KafkaTransmitted}
import io.divby0.pearlriver.modules.KafkaConsumerSettings
import com.typesafe.config.Config

import scala.concurrent.Future

/**
  * Hoolix 2017
  * Created by simon on 1/1/17.
  */
object KafkaSource {

  def convertToEvent(committableMessage: CommittableMessage[Array[Byte], String]): Future[KafkaTransmitted] = {
    val event = KafkaTransmitted(
      committableMessage.committableOffset,
      FileBeatEvent.fromJsonString(committableMessage.record.value)
    )
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


