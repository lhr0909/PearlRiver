package com.hoolix.processor.flows

import java.util
import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.hoolix.processor.models._
import com.hoolix.processor.sinks.ReactiveKafkaSink
import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.unit.{ByteSizeUnit, ByteSizeValue, TimeValue}

import scala.concurrent.{ExecutionContext, Future, Promise}

/**
  * Hoolix 2017
  * Created by simon on 1/1/17.
  */
object ElasticsearchBulkRequestFlow {
  def apply[SrcMeta <: SourceMetadata](
                                        elasticsearchClient: TransportClient,
                                        concurrentRequests: Int,
                                        reactiveKafkaSink: ReactiveKafkaSink
                                      )(implicit config: Config, ec: ExecutionContext): ElasticsearchBulkRequestSink[SrcMeta] = {

    val esBulkConfig = config.getConfig("elasticsearch.bulk")
    val maxBulkSizeInBytes = esBulkConfig.getString("max-size-in-bytes")
    val maxBulkActions = esBulkConfig.getInt("max-actions")
    val bulkTimeout = esBulkConfig.getString("timeout")

    new ElasticsearchBulkRequestSink[SrcMeta](
      elasticsearchClient,
      maxBulkSizeInBytes,
      maxBulkActions,
      bulkTimeout,
      concurrentRequests,
      reactiveKafkaSink,
      ec
    )
  }

  class ElasticsearchBulkRequestSink[SrcMeta <: SourceMetadata](
                                      elasticsearchClient: TransportClient,
                                      maxBulkSizeInBytes: String,
                                      maxBulkActions: Int,
                                      bulkTimeoutTimeValue: String,
                                      concurrentRequests: Int,
                                      reactiveKafkaSink: ReactiveKafkaSink,
                                      implicit val ec: ExecutionContext
                                    ) {

    type Shipperz = Shipper[SrcMeta, ElasticsearchPortFactory]
    type SuccessAndFailureOffsets = (Seq[SrcMeta], Seq[SrcMeta])

    def bulkFlow: Flow[Shipperz, Option[ElasticsearchBulkRequestContainer[SrcMeta]], NotUsed] = {
      Flow[Shipperz].via(ElasticsearchBulkProcessingFlow[SrcMeta](
        maxBulkActions,
        ByteSizeValue.parseBytesSizeValue(
          maxBulkSizeInBytes,
          new ByteSizeValue(5, ByteSizeUnit.MB),
          "elasticsearch.bulk.max-size-in-bytes"
        ),
        TimeValue.parseTimeValue(
          bulkTimeoutTimeValue,
          new TimeValue(60, TimeUnit.SECONDS),
          "elasticsearch.bulk.timeout"
        ).getMillis
      )).named("es-bulk-flow")
    }

    def bulkListener(callback: (Any => Unit)): ActionListener[BulkResponse] = new ActionListener[BulkResponse] {
      override def onFailure(e: Exception): Unit = {
        callback(e)
      }

      override def onResponse(response: BulkResponse): Unit = {
        callback(response)
      }
    }

    def mergeOffsets(finalMap: util.Map[TopicPartition, OffsetAndMetadata], sourceMetadata: SrcMeta): util.Map[TopicPartition, OffsetAndMetadata] = {
      sourceMetadata match {
        case ks: KafkaSourceMetadata =>
          val consumerRecord = ks.offset
          val topicPartition = new TopicPartition(consumerRecord.topic(), consumerRecord.partition())
          val offsetAndMetadata = new OffsetAndMetadata(consumerRecord.offset(), "xyz-processor")
          finalMap.put(topicPartition, offsetAndMetadata)
      }

      finalMap
    }

    def makeBulkRequest(bulkRequestContainer: Option[ElasticsearchBulkRequestContainer[SrcMeta]]): Future[Option[ElasticsearchBulkRequestContainer[SrcMeta]]] = {
      val bulkRequestPromise = Promise[Option[ElasticsearchBulkRequestContainer[SrcMeta]]]()

      bulkRequestContainer match {
        case Some(container) =>

          Future {
            println(s"Submitting ES Bulk Request #${ container.id } of size ${ container.bulkRequest.requests().size() }")

            elasticsearchClient.bulk(container.bulkRequest, bulkListener {
              case bulkResponse: BulkResponse =>
                println(s"Bulk Request #${ container.id } finished. Time took - ${ bulkResponse.getTookInMillis }" )
                bulkRequestPromise.success(Some(ElasticsearchBulkRequestContainer(
                  container.id,
                  container.bulkRequest,
                  Some(bulkResponse),
                  None,
                  container.shippers
                )))
              case e: Exception =>
                //FIXME: this is error for the bulk request, failure here stops the stream
                println(s"Bulk Request #${ container.id } failed" )
                e.printStackTrace()
                //bulkRequestPromise.failure(e)
                bulkRequestPromise.success(Some(ElasticsearchBulkRequestContainer(
                  container.id,
                  container.bulkRequest,
                  None,
                  Some(e),
                  container.shippers
                )))
            })
          }

        case _ => bulkRequestPromise.success(None)
      }

      bulkRequestPromise.future
    }

    def flow(): Flow[Shipperz, Option[ElasticsearchBulkRequestContainer[SrcMeta]], NotUsed] = {
      if (concurrentRequests < 1) {
        bulkFlow
          .filter(_.isDefined)
          .mapAsync[Option[ElasticsearchBulkRequestContainer[SrcMeta]]](1)(makeBulkRequest).named("make-bulk-request-mapAsync-single")
          .filter(_.isDefined)
      } else {
        bulkFlow
          .filter(_.isDefined)
          .mapAsync[Option[ElasticsearchBulkRequestContainer[SrcMeta]]](concurrentRequests)(makeBulkRequest).named("make-bulk-request-mapAsync-parallel")
          .filter(_.isDefined)
      }
    }
  }

}

