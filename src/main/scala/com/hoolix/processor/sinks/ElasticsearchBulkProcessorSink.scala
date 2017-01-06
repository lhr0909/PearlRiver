package com.hoolix.processor.sinks

import akka.NotUsed
import akka.kafka.ConsumerMessage.CommittableOffsetBatch
import akka.stream.scaladsl.{Flow, Sink}
import com.hoolix.processor.models.KafkaEvent
import com.hoolix.elasticsearch.action.bulk.BulkProcessor.Listener
import com.hoolix.elasticsearch.action.bulk.BulkProcessor
import com.typesafe.config.Config
import org.elasticsearch.action.bulk.{BackoffPolicy, BulkRequest, BulkResponse}
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.unit.{ByteSizeUnit, ByteSizeValue, TimeValue}

import scala.collection.immutable
import scala.concurrent.ExecutionContext

/**
  * Hoolix 2017
  * Created by simon on 1/1/17.
  */
object ElasticsearchBulkProcessorSink {
  def apply(
    elasticsearchClient: TransportClient,
    concurrentRequests: Int
  )(implicit config: Config, ec: ExecutionContext): ElasticsearchBulkProcessorSink = {
    val esBulkConfig = config.getConfig("elasticserach.bulk")
    val maxBulkSizeInBytes = esBulkConfig.getString("max-size-in-bytes")
    val maxBulkActions = esBulkConfig.getInt("max-actions")

    new ElasticsearchBulkProcessorSink(elasticsearchClient, maxBulkSizeInBytes, maxBulkActions, concurrentRequests, ec)
  }

  class ElasticsearchBulkProcessorSink(
                                      elasticsearchClient: TransportClient,
                                      maxBulkSizeInBytes: String,
                                      maxBulkSize: Int,
                                      concurrentRequests: Int,
                                      implicit val ec: ExecutionContext
                                    ) {

    val bulkProcessor: BulkProcessor = BulkProcessor.builder(elasticsearchClient, new Listener {
      override def beforeBulk(executionId: Long, request: BulkRequest): Unit = {
        println(s"Bulk Request #$executionId - " + request.numberOfActions + " events being indexed")
      }

      override def afterBulk(
                              executionId: Long,
                              request: BulkRequest,
                              response: BulkResponse,
                              offsetBatch: CommittableOffsetBatch
                            ): Unit = {

        println(s"bulk result for Bulk Request #$executionId")
        println("bulk size - " + response.getItems.length)
        println("bulk time - " + response.getTookInMillis)
        println("end bulk result")
        println("committing kafka offsets - " + offsetBatch.offsets())
        offsetBatch.commitScaladsl()

      }

      override def afterBulk(executionId: Long, request: BulkRequest, failure: Throwable): Unit = {
        throw failure
      }
    }).setBulkActions(maxBulkSize)
      .setBulkSize(ByteSizeValue.parseBytesSizeValue(
        maxBulkSizeInBytes,
        new ByteSizeValue(5, ByteSizeUnit.MB),
        "elasticsearch.bulk.max-size-in-bytes"
      ))
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

    def processKafkaEvent(event: KafkaEvent): Unit = {
      bulkProcessor.add(event.toIndexRequest.source(event.toPayload), event.getCommittableOffset)
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

}

