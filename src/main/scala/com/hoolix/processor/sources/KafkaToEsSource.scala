package com.hoolix.processor.sources
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.Subscriptions
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.scaladsl.Source
import com.hoolix.processor.models._
import com.hoolix.processor.models.events.FileBeatEvent
import com.hoolix.processor.modules.KafkaConsumerSettings

import scala.concurrent.Future

/**
  * Hoolix 2017
  * Created by simon on 1/14/17.
  */
case class KafkaToEsSource(parallel: Int, kafkaTopic: String) extends AbstractSource[KafkaSourceMetadata, ElasticsearchPortFactory] {

  override type S = CommittableMessage[Array[Byte], String]
  override type Mat = Control

  override val sourceType: String = "kafka"

  override val parallelism: Int = parallel

  //FIXME: rolling back to single topic per stream for now, need more fine-grained control
  override val startingSource: Source[CommittableMessage[Array[Byte], String], Control] =
    Consumer.committableSource(KafkaConsumerSettings(), Subscriptions.topics(Set(kafkaTopic)))

  override def convertToShipper(incoming: CommittableMessage[Array[Byte], String]): Future[Shipper[KafkaSourceMetadata, ElasticsearchPortFactory]] = {
    Future.successful(
      Shipper(
        FileBeatEvent.fromJsonString(incoming.record.value),
        KafkaSourceMetadata(incoming.committableOffset),
        ElasticsearchPortFactory()
      )
    )
  }

}
