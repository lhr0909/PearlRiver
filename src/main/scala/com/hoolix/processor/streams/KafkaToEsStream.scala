package com.hoolix.processor.streams

import akka.kafka.ConsumerSettings
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, RunnableGraph}
import com.hoolix.elasticsearch.action.bulk.BulkProcessor
import com.hoolix.processor.decoders.{FileBeatDecoder, XYZLineDecoder}
import com.hoolix.processor.filters.Filter
import com.hoolix.processor.flows.{DecodeFlow, FilterFlow}
import com.hoolix.processor.sinks.ElasticsearchBulkRequestSink
import com.hoolix.processor.sources.KafkaSource
import org.elasticsearch.client.transport.TransportClient

import scala.concurrent.ExecutionContext

/**
  * Hoolix 2017
  * Created by simon on 1/4/17.
  */
object KafkaToEsStream {
  def apply(
             parallelism: Int,
             maxSize: Int,
             esClient: TransportClient,
             kafkaConsumerSettings: ConsumerSettings[Array[Byte], String],
             kafkaTopics: Set[String],
             ec: ExecutionContext
           ): KafkaToEsStream = new KafkaToEsStream(parallelism, maxSize, esClient, kafkaConsumerSettings, kafkaTopics, ec)

  class KafkaToEsStream(
                         parallelism: Int,
                         maxSize: Int,
                         esClient: TransportClient,
                         kafkaConsumerSettings: ConsumerSettings[Array[Byte], String],
                         kafkaTopics: Set[String],
                         implicit val ec: ExecutionContext
                       ) {

    val kafkaSource = KafkaSource(parallelism, kafkaConsumerSettings, kafkaTopics)
    val esSink = ElasticsearchBulkRequestSink(esClient, maxSize, parallelism, ec)

    def stream: RunnableGraph[Control] = {
      val decodeFlow = DecodeFlow(20, FileBeatDecoder())
      val filterFlow = FilterFlow(20, Seq[Filter]())

      //    val mainStream = kafkaSource.toSource.via(decodeFlow.toFlow).via(filterFlow.toFlow).toMat(esSink.toSink)


      kafkaSource.viaMat(decodeFlow.toFlow)(Keep.left).viaMat(filterFlow.toFlow)(Keep.left).toMat(esSink.sink)(Keep.left)
    }

    def run()(implicit materializer: Materializer): (BulkProcessor, Control) = {
      val kafkaControl = stream.run()
      (esSink.bulkProcessor, kafkaControl)
    }
  }
}


