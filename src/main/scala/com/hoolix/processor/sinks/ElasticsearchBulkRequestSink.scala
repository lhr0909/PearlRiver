package com.hoolix.processor.sinks

import akka.NotUsed
import akka.kafka.ConsumerMessage.CommittableOffsetBatch
import akka.stream.scaladsl.{Flow, Sink}
import com.hoolix.processor.models.Event
import org.elasticsearch.action.bulk.BulkProcessor.Listener
import org.elasticsearch.action.bulk.{BackoffPolicy, BulkProcessor, BulkRequest, BulkResponse}
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.unit.{ByteSizeUnit, ByteSizeValue, TimeValue}

import scala.collection.immutable
import scala.concurrent.ExecutionContext

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

  var bulking: Boolean = false
  var bulkBuffer: Seq[Event] = Seq()
  var bulkingBuffer: Seq[Event] = Seq()
  var bulkRequestListener: Listener = _

  val bulkProcessor: BulkProcessor = BulkProcessor.builder(elasticsearchClient, new Listener {
    override def beforeBulk(executionId: Long, request: BulkRequest) = {
      println("# of bulks being flushed - " + request.numberOfActions)
      println("# of bulks in buffer for commit - " + bulkBuffer.length)
      bulking = true
    }

    override def afterBulk(executionId: Long, request: BulkRequest, response: BulkResponse) = {
      bulking = false
      println("bulk result")
      println("bulk size - " + response.getItems.length)
      println("bulk time - " + response.getTookInMillis)
      println("end bulk result")
      val offsetBatch = bulkBuffer.foldLeft(CommittableOffsetBatch.empty) { (batch, elem) => batch.updated(elem.getCommittableOffset) }
      println("offset batch size - " + offsetBatch.offsets().size)
      offsetBatch.commitScaladsl()
      bulkBuffer = Seq()
    }

    override def afterBulk(executionId: Long, request: BulkRequest, failure: Throwable) = {
      bulking = false
      throw failure
    }
  })
    .setBulkActions(maxBulkSize)
    .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
    .setConcurrentRequests(concurrentRequests)
    .setBackoffPolicy(
      BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3)
    )
    .build()

  def startingFlow: Flow[Event, Seq[Event], NotUsed] = {
    if (maxBulkSize < 0) {
      Flow[Event].conflateWithSeed(Seq(_))(_ :+ _)
    } else {
      Flow[Event].batch(maxBulkSize, Seq(_))(_ :+ _)
    }
  }

  def processEvent(event: Event): Unit = {
    bulkProcessor.add(event.toIndexRequest.source(event.toPayload))
    if (!bulking) {
      if (bulkingBuffer.nonEmpty) {
        bulkBuffer = bulkBuffer ++ bulkingBuffer
        bulkingBuffer = Seq()
      }
      bulkBuffer = bulkBuffer :+ event
    } else {
      bulkingBuffer = bulkingBuffer :+ event
    }
  }

  def toSink: Sink[Event, NotUsed] = {
    val flow = startingFlow.mapConcat(_.to[immutable.Seq])
    if (concurrentRequests < 1) {
      flow.to(Sink.foreach(processEvent))
    } else {
      flow.to(Sink.foreachParallel(concurrentRequests)(processEvent))
    }
  }
}


