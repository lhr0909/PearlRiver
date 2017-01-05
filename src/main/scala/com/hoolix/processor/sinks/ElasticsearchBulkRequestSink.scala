package com.hoolix.processor.sinks

import akka.NotUsed
import akka.kafka.ConsumerMessage.CommittableOffsetBatch
import akka.stream.scaladsl.{Flow, Sink}
import com.hoolix.processor.models.Event
import org.elasticsearch.action.bulk.BulkProcessor.Listener
import org.elasticsearch.action.bulk.{BackoffPolicy, BulkProcessor, BulkRequest, BulkResponse}
import com.hoolix.elasticsearch.action.bulk.BulkProcessor.Listener
import com.hoolix.elasticsearch.action.bulk.BulkProcessor
import org.elasticsearch.action.bulk.{BackoffPolicy, BulkRequest, BulkResponse}
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.unit.{ByteSizeUnit, ByteSizeValue, TimeValue}

import scala.collection.immutable
import scala.concurrent.ExecutionContext

/**
  * Hoolix 2017
  * Created by simon on 1/1/17.
  */
object ElasticsearchBulkRequestSink {
  def apply(
    elasticsearchClient: TransportClient,
    maxBulkSize: Int,
    concurrentRequests: Int,
    ec: ExecutionContext
  ) = new ElasticsearchBulkRequestSink(elasticsearchClient, maxBulkSize, concurrentRequests, ec)

<<<<<<< HEAD
  var bulking: Boolean = false
  var bulkBuffer: Seq[Event] = Seq()
  var bulkingBuffer: Seq[Event] = Seq()
  var bulkRequestListener: Listener = _
=======
  class ElasticsearchBulkRequestSink(
                                      elasticsearchClient: TransportClient,
                                      maxBulkSize: Int,
                                      concurrentRequests: Int,
                                      implicit val ec: ExecutionContext
                                    ) {
>>>>>>> dev

    val bulkProcessor: BulkProcessor = BulkProcessor.builder(elasticsearchClient, new Listener {
      override def beforeBulk(executionId: Long, request: BulkRequest) = {
        println(s"Bulk Request #$executionId - " + request.numberOfActions + " events being indexed")
      }

      override def afterBulk(
                              executionId: Long,
                              request: BulkRequest,
                              response: BulkResponse,
                              offsetBatch: CommittableOffsetBatch
                            ) = {

        println(s"bulk result for Bulk Request #$executionId")
        println("bulk size - " + response.getItems.length)
        println("bulk time - " + response.getTookInMillis)
        println("end bulk result")
        println("committing kafka offsets - " + offsetBatch.offsets())
        offsetBatch.commitScaladsl()

      }

      override def afterBulk(executionId: Long, request: BulkRequest, failure: Throwable) = {
        throw failure
      }
    }).setBulkActions(maxBulkSize)
      .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
      .setConcurrentRequests(concurrentRequests)
      .setBackoffPolicy(
        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3)
      ).build()

    def startingFlow: Flow[KafkaEvent, Seq[KafkaEvent], NotUsed] = {
      if (maxBulkSize < 0) {
        Flow[KafkaEvent].conflateWithSeed(Seq(_))(_ :+ _)
      } else {
        Flow[KafkaEvent].batch(maxBulkSize, Seq(_))(_ :+ _)
      }
    }

<<<<<<< HEAD
  def startingFlow: Flow[Event, Seq[Event], NotUsed] = {
    if (maxBulkSize < 0) {
      Flow[Event].conflateWithSeed(Seq(_))(_ :+ _)
    } else {
      Flow[Event].batch(maxBulkSize, Seq(_))(_ :+ _)
=======
    def processKafkaEvent(event: KafkaEvent): Unit = {
      bulkProcessor.add(event.toIndexRequest.source(event.toPayload), event.getCommittableOffset)
>>>>>>> dev
    }

<<<<<<< HEAD
  def processEvent(event: Event): Unit = {
    bulkProcessor.add(event.toIndexRequest.source(event.toPayload))
    if (!bulking) {
      if (bulkingBuffer.nonEmpty) {
        bulkBuffer = bulkBuffer ++ bulkingBuffer
        bulkingBuffer = Seq()
=======
    def sink: Sink[KafkaEvent, NotUsed] = {
      val flow = startingFlow.mapConcat(_.to[immutable.Seq])
      if (concurrentRequests < 1) {
        flow.to(Sink.foreach(processKafkaEvent))
      } else {
        flow.to(Sink.foreachParallel(concurrentRequests)(processKafkaEvent))
>>>>>>> dev
      }
    }
  }

<<<<<<< HEAD
  def toSink: Sink[Event, NotUsed] = {
    val flow = startingFlow.mapConcat(_.to[immutable.Seq])
    if (concurrentRequests < 1) {
      flow.to(Sink.foreach(processEvent))
    } else {
      flow.to(Sink.foreachParallel(concurrentRequests)(processEvent))
    }
  }
=======
>>>>>>> dev
}

