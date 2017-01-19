package com.hoolix.processor.streams

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.{KillSwitches, Materializer, UniqueKillSwitch}
import akka.stream.scaladsl.{Keep, RunnableGraph}
import com.hoolix.processor.decoders.FileBeatDecoder
import com.hoolix.processor.filters.loaders.ConfigLoader
import com.hoolix.processor.flows._
import com.hoolix.processor.models.{ElasticsearchPortFactory, KafkaSourceMetadata, Shipper}
import com.hoolix.processor.modules.ElasticsearchClient
import com.hoolix.processor.sinks.ReactiveKafkaSink
import com.hoolix.processor.sources.{KafkaToEsSource, ReactiveKafkaSource}
import com.typesafe.config.Config
import org.elasticsearch.client.transport.TransportClient

import scala.collection.JavaConversions
import scala.concurrent.{ExecutionContext, Future}

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

    val futureExecutionContext = system.dispatchers.lookup("future-dispatcher")

    val topicsJavaSet = JavaConversions.setAsJavaSet(kafkaTopics)
    val reactiveKafkaSource = new ReactiveKafkaSource(topicsJavaSet)
    val reactiveKafkaSink = new ReactiveKafkaSink(topicsJavaSet)

    val kafkaSource = KafkaToEsSource(parallelism, reactiveKafkaSource)

    val esBulkRequestSink = ElasticsearchBulkRequestFlow[KafkaSourceMetadata](
      esClient,
      parallelism,
      reactiveKafkaSink
    )(config, futureExecutionContext)

    def stream: RunnableGraph[(UniqueKillSwitch, Future[Done])] = {

      val decodeFlow = DecodeFlow[KafkaSourceMetadata, ElasticsearchPortFactory](parallelism, FileBeatDecoder()).flow
      val filtersLoadFlow = FiltersLoadFlow[KafkaSourceMetadata, ElasticsearchPortFactory](parallelism).flow
      val filterFlow = FilterFlow[KafkaSourceMetadata, ElasticsearchPortFactory](parallelism).flow()

      val createIndexFlow = ElasticsearchCreateIndexFlow[KafkaSourceMetadata](
          parallelism,
          esClient,
          ElasticsearchClient.esIndexCreationSettings()
        ).flow(futureExecutionContext)

      kafkaSource.source().named("kafka-to-es-source")
        .viaMat(KillSwitches.single[Shipper[KafkaSourceMetadata, ElasticsearchPortFactory]])(Keep.right).named("kill-switch")
        .viaMat(decodeFlow)(Keep.left).named("kafka-to-es-decode-flow")
        .viaMat(filtersLoadFlow)(Keep.left).named("kafka-to-es-filters-load-flow")
        .viaMat(filterFlow)(Keep.left).named("kafka-to-es-filter-flow")
        .viaMat(createIndexFlow)(Keep.left).named("kafka-to-es-index-flow")
        .toMat(esBulkRequestSink.sink())(Keep.both).named("kafka-to-es-sink")
    }

  }
}
