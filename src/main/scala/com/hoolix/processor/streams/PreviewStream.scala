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
import com.hoolix.processor.models.{DummyPortFactory, DummySourceMetadata, Shipper}
import com.hoolix.processor.models.events.LineEvent
import com.hoolix.processor.modules.ElasticsearchClient
import com.hoolix.processor.sinks.ElasticsearchBulkRequestSink
import com.hoolix.processor.sources.KafkaToEsSource
import com.typesafe.config.Config
import org.elasticsearch.client.transport.TransportClient

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by peiyuchao on 2017/1/11.
  */
class PreviewStream(
                     sample: Seq[String],
                     token: String,
                     `type`: String,
                     filters: Seq[ConditionedFilter],
                     parallelism: Int,
                     implicit val config: Config,
                     implicit val system: ActorSystem,
                     implicit val ec: ExecutionContext
                   ) {

  val source: Source[Shipper[DummySourceMetadata, DummyPortFactory], NotUsed] = Source.fromIterator(() => sample.toIterator).mapAsync(parallelism) {
    (message) => Future.successful(
      Shipper(
        LineEvent(message),
        DummySourceMetadata(),
        DummyPortFactory()
      )
    )
  }
  val decodePreviewFlow = DecodePreviewFlow[DummySourceMetadata, DummyPortFactory](parallelism, RawLineDecoder(token, `type`, Seq(), "batch")).flow
  val filtersLoadPreviewFlow = FiltersLoadPreviewFlow[DummySourceMetadata, DummyPortFactory](parallelism, filters).flow
  val filterPreviewFlow = FilterPreviewFlow[DummySourceMetadata, DummyPortFactory](parallelism).flow()
//  val sink = Sink.foreach(println)

  def stream = {
    source
      .viaMat(decodePreviewFlow)(Keep.left)
      .viaMat(filtersLoadPreviewFlow)(Keep.left)
      .viaMat(filterPreviewFlow)(Keep.left)
//      .toMat(sink)(Keep.left)
  }

//  def run()(implicit materializer: Materializer): Unit = {
//    stream.run()
//  }
}
