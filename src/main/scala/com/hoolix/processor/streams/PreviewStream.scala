package com.hoolix.processor.streams
import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, RunnableGraph, Sink, Source}
import com.hoolix.processor.decoders.{FileBeatDecoder, RawLineDecoder}
import com.hoolix.processor.filters.loaders.ConfigLoader
import com.hoolix.processor.filters.Filter.ConditionedFilter
import com.hoolix.processor.flows._
import com.hoolix.processor.models.LineEvent
import com.hoolix.processor.modules.ElasticsearchClient
import com.hoolix.processor.sinks.ElasticsearchBulkRequestSink
import com.hoolix.processor.sources.KafkaSource
import com.typesafe.config.Config
import org.elasticsearch.client.transport.TransportClient

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by peiyuchao on 2017/1/11.
  */
class PreviewStream(
                     sample: String,
                     token: String,
                     `type`: String,
                     filters: Seq[ConditionedFilter],
                     parallelism: Int,
                     implicit val config: Config,
                     implicit val system: ActorSystem,
                     implicit val ec: ExecutionContext
                   ) {

  val source: Source[LineEvent, NotUsed] = Source.fromIterator(() => sample.split("\n").toIterator).mapAsync(parallelism) {
    (message) => Future.successful(LineEvent(message))
  }
  val decodePreviewFlow = DecodePreviewFlow(parallelism, RawLineDecoder(token, `type`, Seq(), "batch"))
  val filtersLoadPreviewFlow = FiltersLoadPreviewFlow(parallelism, filters)
  val filterPreviewFlow = FilterPreviewFlow(parallelism)
  val sink = Sink.foreach(println)

  def stream = {
    source
      .viaMat(decodePreviewFlow)(Keep.left)
      .viaMat(filtersLoadPreviewFlow)(Keep.left)
      .viaMat(filterPreviewFlow)(Keep.left)
      .toMat(sink)(Keep.left)
  }

  def run()(implicit materializer: Materializer): Unit = {
    stream.run()
  }
//
//  source.mapAsync(parallelism, )
//  val sink = println
//
//  def Stream: RunnableGraph[Control] = {
//
//    source
//      .viaMat(decodeFlow)(Keep.left)
//      .viaMat(FiltersLoadFlow(parallelism))(Keep.left)
//      .viaMat(filterFlow)(Keep.left)
//      .viaMat(CreateIndexFlow(
//        parallelism,
//        esClient,
//        ElasticsearchClient.esIndexCreationSettings()
//      ))(Keep.left)
//      .toMat(esSink.sink)(Keep.left)
//  }



//  //TODO: get stream cache into SQL
//
//  val streamCache: TrieMap[String, Control] = TrieMap()
//
//  def apply(
//             parallelism: Int,
//             esClient: TransportClient,
//             kafkaTopic: String
//           )(implicit config: Config, system: ActorSystem, ec: ExecutionContext): KafkaToEsStream =
//    new KafkaToEsStream(parallelism, esClient, kafkaTopic, config, system, ec)
//
//
//
//
//    val kafkaSource = KafkaSource(parallelism, kafkaTopic)
//    //    val esSink = ElasticsearchBulkProcessorSink(esClient, parallelism)
//    val esSink = ElasticsearchBulkRequestSink(esClient, parallelism)
//
//    def stream: RunnableGraph[Control] = {
//
//      val decodeFlow = DecodeFlow(parallelism, FileBeatDecoder())
//
//
//      val filterFlow = FilterFlow(parallelism)
//
//      kafkaSource
//        .viaMat(decodeFlow)(Keep.left)
//        .viaMat(FiltersLoadFlow(parallelism))(Keep.left)
//        .viaMat(filterFlow)(Keep.left)
//        .viaMat(CreateIndexFlow(
//          parallelism,
//          esClient,
//          ElasticsearchClient.esIndexCreationSettings()
//        ))(Keep.left)
//        .toMat(esSink.sink)(Keep.left)
//    }
}
