package io.divby0.pearlriver.sinks

import akka.NotUsed
import akka.kafka.ConsumerMessage.CommittableOffsetBatch
import akka.stream.scaladsl.{Flow, Sink}
import io.divby0.elasticsearch.action.bulk.BulkProcessor.Listener
import io.divby0.pearlriver.models.KafkaTransmitted
import com.typesafe.config.Config
import io.divby0.elasticsearch.action.bulk.BulkProcessor
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

    def startingFlow: Flow[KafkaTransmitted, Seq[KafkaTransmitted], NotUsed] = {
      if (maxBulkSize < 0) {
        Flow[KafkaTransmitted].conflateWithSeed(Seq(_))(_ :+ _)
      } else {
        Flow[KafkaTransmitted].batch(maxBulkSize, Seq(_))(_ :+ _)
      }
    }

    def processKafkaTransmitted(event: KafkaTransmitted): Unit = {
      bulkProcessor.add(event.toIndexRequest.source(event.event.toPayload), event.committableOffset)
    }

    def sink: Sink[KafkaTransmitted, NotUsed] = {
      val flow = startingFlow.mapConcat(_.to[immutable.Seq])
      if (concurrentRequests < 1) {
        flow.to(Sink.foreach(processKafkaTransmitted))
      } else {
        flow.to(Sink.foreachParallel(concurrentRequests)(processKafkaTransmitted))
      }
    }
  }

}

