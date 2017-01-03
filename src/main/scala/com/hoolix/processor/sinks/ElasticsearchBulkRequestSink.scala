package com.hoolix.processor.sinks

import akka.NotUsed
import akka.actor.ActorRef
import akka.kafka.ConsumerMessage.CommittableOffsetBatch
import akka.stream.scaladsl.{Flow, Sink, SourceQueueWithComplete}
import com.hoolix.processor.models.KafkaEvent
import com.hoolix.processor.sinks.ElasticsearchBulkRequestSink.AfterBulkTrigger
import com.hoolix.processor.streams.KafkaOffsetCommitStream
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
object ElasticsearchBulkRequestSink {
  case class BeforeBulkTrigger()
  case class AfterBulkTrigger()
}

case class ElasticsearchBulkRequestSink(
                                       elasticsearchClient: TransportClient,
                                       maxBulkSize: Int,
                                       concurrentRequests: Int,
                                       kafkaOffsetCommitStreamContext: KafkaOffsetCommitStream.MaterializedContext,
                                       implicit val ec: ExecutionContext
                                       ) {

  var bulking: Boolean = false
  val (bulkQueue, bulkingQueue, afterBulkTriggerActor) = kafkaOffsetCommitStreamContext
  val bulkProcessor: BulkProcessor = BulkProcessor.builder(elasticsearchClient, new Listener {
    override def beforeBulk(executionId: Long, request: BulkRequest) = {
      println("# of bulks being flushed - " + request.numberOfActions)
      bulking = true
    }

    override def afterBulk(executionId: Long, request: BulkRequest, response: BulkResponse) = {
      bulking = false
      afterBulkTriggerActor ! AfterBulkTrigger
      println("bulk result")
      println("bulk size - " + response.getItems.length)
      println("bulk time - " + response.getTookInMillis)
      println("end bulk result")
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

  def startingFlow: Flow[KafkaEvent, Seq[KafkaEvent], NotUsed] = {
    if (maxBulkSize < 0) {
      Flow[KafkaEvent].conflateWithSeed(Seq(_))(_ :+ _)
    } else {
      Flow[KafkaEvent].batch(maxBulkSize, Seq(_))(_ :+ _)
    }
  }

  def processKafkaEvent(event: KafkaEvent): Unit = {
    bulkProcessor.add(event.toIndexRequest.source(event.toPayload))
    if (!bulking) {
      bulkQueue.offer(event)
    } else {
      bulkingQueue.offer(event)
    }
  }

  def sink: Sink[KafkaEvent, NotUsed] = {
    val flow = startingFlow.mapConcat(_.to[immutable.Seq])
    if (concurrentRequests < 1) {
      flow.to(Sink.foreach(processKafkaEvent))
    } else {
      flow.to(Sink.foreachParallel(concurrentRequests)(processKafkaEvent))
    }
  }
}


