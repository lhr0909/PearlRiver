package com.hoolix.processor.streams

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.scaladsl.{Keep, RunnableGraph}
import akka.stream.{KillSwitch, KillSwitches, Materializer}
import com.hoolix.processor.sinks.ElasticsearchBulkRequestSink
import com.hoolix.processor.sources.KafkaSource
import com.typesafe.config.Config
import org.elasticsearch.client.transport.TransportClient

import scala.concurrent.ExecutionContext

/**
  * Hoolix 2017
  * Created by simon on 1/4/17.
  */
object KafkaToEsStream {
  def apply(
             parallelism: Int,
             esClient: TransportClient,
             kafkaTopics: Set[String]
           )(implicit config: Config, system: ActorSystem, ec: ExecutionContext): KafkaToEsStream =
    new KafkaToEsStream(parallelism, esClient, kafkaTopics, config, system, ec)

  class KafkaToEsStream(
                         parallelism: Int,
                         esClient: TransportClient,
                         kafkaTopics: Set[String],
                         implicit val config: Config,
                         implicit val system: ActorSystem,
                         implicit val ec: ExecutionContext
                       ) {

    val kafkaSource = KafkaSource(parallelism, kafkaTopics)
//    val esSink = ElasticsearchBulkProcessorSink(esClient, parallelism)
    val esSink = ElasticsearchBulkRequestSink(esClient, parallelism)

    def stream: RunnableGraph[Control] = {
      kafkaSource.toMat(esSink.sink)(Keep.left)
    }

//    def run()(implicit materializer: Materializer): (BulkProcessor, Control) = {
//      val kafkaControl = stream.run()
//      (esSink.bulkProcessor, kafkaControl)
//    }
//
    def run()(implicit materializer: Materializer): Control = stream.run()

  }
}


