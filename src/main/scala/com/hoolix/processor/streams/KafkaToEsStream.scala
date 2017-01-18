package com.hoolix.processor.streams

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, RunnableGraph}
import com.hoolix.processor.decoders.FileBeatDecoder
import com.hoolix.processor.models.{ElasticsearchPortFactory, KafkaSourceMetadata}
import com.hoolix.processor.flows.{CreateIndexFlow, DecodeFlow, FilterFlow, FiltersLoadFlow}
import com.hoolix.processor.modules.ElasticsearchClient
import com.hoolix.processor.sinks.ElasticsearchBulkRequestSink
import com.hoolix.processor.sources.KafkaToEsSource
import com.typesafe.config.Config
import org.elasticsearch.client.transport.TransportClient

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext
import scala.util.Try

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
             kafkaTopic: String,
             onComplete: (Try[Done]) => Unit
           )(implicit config: Config, system: ActorSystem, ec: ExecutionContext): KafkaToEsStream =
    new KafkaToEsStream(parallelism, esClient, kafkaTopic, onComplete, config, system, ec)

  //TODO: add a callback when it is done
  def stopStream(kafkaTopic: String)(implicit ec: ExecutionContext): Unit = {
    //FIXME: we are only stopping here, will need to shutdown the whole stream when it is completely done
    streamCache(kafkaTopic).stop()
  }

  def shutdownStream(kafkaTopic: String)(implicit ec: ExecutionContext): Unit = {
    streamCache(kafkaTopic).shutdown() onSuccess {
      case Done =>
        streamCache.remove(kafkaTopic)
        println(s"stream for $kafkaTopic has shutdown and cleaned up")
    }
  }

  class KafkaToEsStream(
                         parallelism: Int,
                         esClient: TransportClient,
                         kafkaTopic: String,
                         onComplete: (Try[Done]) => Unit,
                         implicit val config: Config,
                         implicit val system: ActorSystem,
                         implicit val ec: ExecutionContext
                       ) {

    val futureExecutionContext: ExecutionContext = system.dispatchers.lookup("future-dispatcher")

    val kafkaSource = KafkaToEsSource(parallelism, kafkaTopic, config, system)

//    val esBulkProcessorSink = ElasticsearchBulkProcessorSink(esClient, parallelism)(config, futureExecutionContext)
    val esBulkRequestSink = ElasticsearchBulkRequestSink[KafkaSourceMetadata](esClient, parallelism)(config, futureExecutionContext)
    val esSink = esBulkRequestSink
//    val esSink = esBlulkProcessorSink

    def stream: RunnableGraph[Control] = {

      val decodeFlow = DecodeFlow[KafkaSourceMetadata, ElasticsearchPortFactory](parallelism, FileBeatDecoder()).flow
      val filtersLoadFlow = FiltersLoadFlow[KafkaSourceMetadata, ElasticsearchPortFactory](parallelism).flow
      val filterFlow = FilterFlow[KafkaSourceMetadata, ElasticsearchPortFactory](parallelism).flow()

      val createIndexFlow = CreateIndexFlow[KafkaSourceMetadata](
        parallelism,
        esClient,
        ElasticsearchClient.esIndexCreationSettings()
      ).flow(futureExecutionContext)


      kafkaSource.source().named("kafka-to-es-source")
        .viaMat(decodeFlow)(Keep.left).named("kafka-to-es-decode-flow")
        .viaMat(filtersLoadFlow)(Keep.left).named("kafka-to-es-filters-load-flow")
        .viaMat(filterFlow)(Keep.left).named("kafka-to-es-filter-flow")
        .viaMat(createIndexFlow)(Keep.left).named("kafka-to-es-index-flow")
        .toMat(esSink.sink(onComplete))(Keep.left).named("kafka-to-es-sink")
    }

    def run()(implicit materializer: Materializer): Unit = {
      val control = stream.run()
      streamCache.put(kafkaTopic, control)
    }
  }
}


