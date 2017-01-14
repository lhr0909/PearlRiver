package com.hoolix.processor.sinks

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.kafka.ConsumerMessage.CommittableOffsetBatch
import akka.stream.scaladsl.{Flow, Sink}
import com.hoolix.elasticsearch.action.bulk.BulkProcessor.Listener
import com.hoolix.elasticsearch.action.bulk.BulkProcessor
import com.hoolix.processor.models.KafkaTransmitted
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
    val esBulkConfig = config.getConfig("elasticsearch.bulk")
    val maxBulkSizeInBytes = esBulkConfig.getString("max-size-in-bytes")
    val maxBulkActions = esBulkConfig.getInt("max-actions")
    val bulkTimeout = esBulkConfig.getString("timeout")

    new ElasticsearchBulkProcessorSink(elasticsearchClient, maxBulkSizeInBytes, maxBulkActions, bulkTimeout, concurrentRequests, ec)
  }

  class ElasticsearchBulkProcessorSink(
                                      elasticsearchClient: TransportClient,
                                      maxBulkSizeInBytes: String,
                                      maxBulkSize: Int,
                                      bulkTimeoutTimeValue: String,
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
      .setFlushInterval(TimeValue.parseTimeValue(
        bulkTimeoutTimeValue,
        new TimeValue(60, TimeUnit.SECONDS),
        "elasticsearch.bulk.timeout"
      ))
      .setConcurrentRequests(concurrentRequests)
      .setBackoffPolicy(
        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3)
      ).build()

    def startingFlow: Flow[KafkaTransmitted, Seq[KafkaTransmitted], NotUsed] = {
      if (maxBulkSize < 0) {
        Flow[KafkaTransmitted].conflateWithSeed(Seq(_))(_ :+ _)
      } else {
        Flow[KafkaTransmitted].batch(maxBulkSize, Seq(_))(_ :+ _)
      }
    }

    def addEventToBulk(incomingEvent: KafkaTransmitted): Unit = {
      bulkProcessor.add(
        incomingEvent.toIndexRequest.source(incomingEvent.event.toPayload),
        incomingEvent.offset
      )
    }

    def sink: Sink[KafkaTransmitted, NotUsed] = {
      val flow = startingFlow.mapConcat(_.to[immutable.Seq])
      if (concurrentRequests < 1) {
        flow.to(Sink.foreach(addEventToBulk)).named("es-bulk-processor-sink-single")
      } else {
        flow.to(Sink.foreachParallel(concurrentRequests)(addEventToBulk)).named("es-bulk-processor-sink-parallel")
      }
    }
  }

}

