package com.hoolix.processor.sources
import akka.NotUsed
import akka.stream.scaladsl.Source
import com.hoolix.processor.models._
import com.hoolix.processor.models.events.FileBeatEvent
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.concurrent.Future

/**
  * Hoolix 2017
  * Created by simon on 1/14/17.
  */
case class KafkaToEsSource(parallel: Int, reactiveKafkaSource: ReactiveKafkaSource) extends AbstractSource[KafkaSourceMetadata, ElasticsearchPortFactory] {

  override type S = ConsumerRecord[String, String]
  override type Mat = NotUsed

  override val sourceType: String = "kafka"

  override val parallelism: Int = parallel

  override val startingSource: Source[ConsumerRecord[String, String], NotUsed] = Source.fromGraph(reactiveKafkaSource)

  override def convertToShipper(incoming: ConsumerRecord[String, String]): Future[Shipper[KafkaSourceMetadata, ElasticsearchPortFactory]] = {
    Future.successful(
      Shipper(
        FileBeatEvent.fromJsonString(incoming.value()),
        KafkaSourceMetadata(incoming),
        ElasticsearchPortFactory()
      )
    )
  }

}
