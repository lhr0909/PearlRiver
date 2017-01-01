package com.hoolix.processor.pipelines.sinks

import akka.{Done, NotUsed}
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Flow, Sink}
import com.hoolix.processor.models.KafkaEvent
import org.elasticsearch.action.bulk.BulkProcessor.Listener
import org.elasticsearch.action.bulk.{BackoffPolicy, BulkProcessor, BulkRequest, BulkResponse}
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.unit.{ByteSizeUnit, ByteSizeValue, TimeValue}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Hoolix 2017
  * Created by simon on 1/1/17.
  */
case class ElasticsearchBulkRequestSink(
                                       elasticsearchClient: TransportClient,
                                       maxBulkSize: Int,
                                       concurrentRequests: Int,
                                       implicit val ec: ExecutionContext
                                       ) {
  val bulkProcessor: BulkProcessor = BulkProcessor.builder(elasticsearchClient, getBulkRequestListener)
    .setBulkActions(maxBulkSize)
    .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
    .setConcurrentRequests(concurrentRequests)
    .setBackoffPolicy(
      BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3)
    )
    .build()

  def getBulkRequestListener: Listener = new Listener {
    override def beforeBulk(executionId: Long, request: BulkRequest) = {
      println("# of bulks being flushed - " + request.numberOfActions)
    }

    override def afterBulk(executionId: Long, request: BulkRequest, response: BulkResponse) = {
      println("bulk result")
      println("bulk size - " + response.getItems.length)
      println("bulk time - " + response.getTookInMillis)
      println("end bulk result")
    }

    override def afterBulk(executionId: Long, request: BulkRequest, failure: Throwable) = {
      throw failure
    }
  }

  def toSink: Sink[KafkaEvent, NotUsed] = {
    val endSink = Flow[KafkaEvent]
      .conflateWithSeed(Seq(_))(_ :+ _) // builds a list
      .to(_getSinkBasedOnParallelism(concurrentRequests))

    if (maxBulkSize < 0) {
      return Flow[KafkaEvent].to(endSink)
    }

    Flow[KafkaEvent]
      .buffer(maxBulkSize, OverflowStrategy.backpressure)
      .to(endSink)
  }

  def _getSinkBasedOnParallelism(concurrentRequests: Int): Sink[Seq[KafkaEvent], Future[Done]] = {
    if (concurrentRequests == 0) {
      return Sink.foreach(_addEventsToBulk(_))
    }
    Sink.foreachParallel(concurrentRequests)(_addEventsToBulk(_))
  }

  def _addEventsToBulk(events: Seq[KafkaEvent]) = {
//    println("batch size - " + events.length)
    events.map { event =>
      bulkProcessor.add(event.toIndexRequest.source(event.toPayload))
    }
  }
}


