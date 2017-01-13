package com.hoolix.processor.streams

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, RunnableGraph}
import com.hoolix.processor.decoders.FileBeatDecoder
import com.hoolix.processor.filters.loaders.ConfigLoader
import com.hoolix.processor.flows.{CreateIndexFlow, DecodeFlow, FilterFlow, FiltersLoadFlow}
import com.hoolix.processor.modules.ElasticsearchClient
import com.hoolix.processor.sinks.ElasticsearchBulkRequestSink
import com.hoolix.processor.sources.KafkaSource
import com.typesafe.config.Config
import org.elasticsearch.client.transport.TransportClient

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

/**
  * Hoolix 2017
  * Created by simon on 1/4/17.
  */
object KafkaToEsStream {

  //TODO: get stream cache into SQL

  val streamCache: TrieMap[String, Control] = TrieMap()

  def apply(
             parallelism: Int,
             esClient: TransportClient,
             kafkaTopic: String
           )(implicit config: Config, system: ActorSystem, ec: ExecutionContext): KafkaToEsStream =
    new KafkaToEsStream(parallelism, esClient, kafkaTopic, config, system, ec)

  //TODO: add a callback when it is done
  def stopStream(kafkaTopic: String)(implicit ec: ExecutionContext): Unit = {
    //FIXME: we are only stopping here, will need to shutdown the whole stream when it is completely done
    streamCache(kafkaTopic).stop()
  }

  def shutdownStream(kafkaTopic: String)(implicit ec: ExecutionContext): Unit = {
    streamCache(kafkaTopic).shutdown() onSuccess {
      case Done =>
        streamCache.remove(kafkaTopic)
    }
  }

  class KafkaToEsStream(
                         parallelism: Int,
                         esClient: TransportClient,
                         kafkaTopic: String,
                         implicit val config: Config,
                         implicit val system: ActorSystem,
                         implicit val ec: ExecutionContext
                       ) {

    val kafkaSource = KafkaSource(parallelism, kafkaTopic)
//    val esSink = ElasticsearchBulkProcessorSink(esClient, parallelism)
    val esSink = ElasticsearchBulkRequestSink(esClient, parallelism)

    def stream: RunnableGraph[Control] = {

      val decodeFlow = DecodeFlow(parallelism, FileBeatDecoder())
      val filterFlow = FilterFlow(parallelism)

      kafkaSource
        .viaMat(decodeFlow)(Keep.left)
        .viaMat(FiltersLoadFlow(parallelism))(Keep.left)
        .viaMat(filterFlow)(Keep.left)
        .viaMat(CreateIndexFlow(
          parallelism,
          esClient,
          ElasticsearchClient.esIndexCreationSettings()
        ))(Keep.left)
        .toMat(esSink.sink)(Keep.left)
    }

    def run()(implicit materializer: Materializer): Unit = {
      val control = stream.run()
      streamCache.put(kafkaTopic, control)
    }
  }
}


