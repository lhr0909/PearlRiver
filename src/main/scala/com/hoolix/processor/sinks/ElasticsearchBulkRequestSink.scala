package com.hoolix.processor.sinks

import akka.NotUsed
import akka.kafka.ConsumerMessage.CommittableOffsetBatch
import akka.stream.scaladsl.{Flow, Sink}
import com.hoolix.processor.models.KafkaEvent
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
  class BeforeBulkTrigger()
  class AfterBulkTrigger()
}

case class ElasticsearchBulkRequestSink(
                                       elasticsearchClient: TransportClient,
                                       maxBulkSize: Int,
                                       implicit val ec: ExecutionContext
                                       ) {

  val bulkProcessor: BulkProcessor = BulkProcessor.builder(elasticsearchClient, new Listener {
    override def beforeBulk(executionId: Long, request: BulkRequest) = {
      println("# of bulks being flushed - " + request.numberOfActions)
    }

    override def afterBulk(executionId: Long, request: BulkRequest, response: BulkResponse, offsetBatch: CommittableOffsetBatch) = {
      println("bulk result")
      println("bulk size - " + response.getItems.length)
      println("bulk time - " + response.getTookInMillis)
      println("end bulk result")
      println("committing kafka offset batch w/ size of " + offsetBatch.offsets().size)
      offsetBatch.commitScaladsl()
    }

    override def afterBulk(executionId: Long, request: BulkRequest, failure: Throwable) = {
      throw failure
    }
  })
    .setBulkActions(maxBulkSize)
    .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
    .setConcurrentRequests(1)
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
    bulkProcessor.add(event.toIndexRequest.source(event.toPayload), event.getCommittableOffset)
  }

  def sink: Sink[KafkaEvent, NotUsed] = {
    startingFlow
      .mapConcat(_.to[immutable.Seq])
      .to(Sink.foreach(processKafkaEvent))
  }
}


