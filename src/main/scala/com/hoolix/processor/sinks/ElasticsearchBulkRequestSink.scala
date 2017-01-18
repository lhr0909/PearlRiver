package com.hoolix.processor.sinks

import java.util.concurrent.TimeUnit

import akka.{Done, NotUsed}
import akka.kafka.ConsumerMessage.{CommittableOffset, CommittableOffsetBatch}
import akka.stream.scaladsl.{Flow, Keep, Sink}
import com.hoolix.processor.flows.ElasticsearchBulkFlow
import com.hoolix.processor.models._
import com.typesafe.config.Config
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.bulk.{BulkItemResponse, BulkRequest, BulkResponse}
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.unit.{ByteSizeUnit, ByteSizeValue, TimeValue}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

/**
  * Hoolix 2017
  * Created by simon on 1/1/17.
  */
object ElasticsearchBulkRequestSink {
  def apply[SrcMeta <: SourceMetadata](
             elasticsearchClient: TransportClient,
             concurrentRequests: Int
           )(implicit config: Config, ec: ExecutionContext): ElasticsearchBulkRequestSink[SrcMeta] = {
    val esBulkConfig = config.getConfig("elasticsearch.bulk")
    val maxBulkSizeInBytes = esBulkConfig.getString("max-size-in-bytes")
    val maxBulkActions = esBulkConfig.getInt("max-actions")
    val bulkTimeout = esBulkConfig.getString("timeout")

    new ElasticsearchBulkRequestSink[SrcMeta](elasticsearchClient, maxBulkSizeInBytes, maxBulkActions, bulkTimeout, concurrentRequests, ec)
  }

  class ElasticsearchBulkRequestSink[SrcMeta <: SourceMetadata](
                                      elasticsearchClient: TransportClient,
                                      maxBulkSizeInBytes: String,
                                      maxBulkActions: Int,
                                      bulkTimeoutTimeValue: String,
                                      concurrentRequests: Int,
                                      implicit val ec: ExecutionContext
                                    ) {

    type BulkRequestAndOffsets = (BulkRequest, Seq[SrcMeta])
    type BulkResponseAndOffsets = (BulkResponse, Seq[SrcMeta])
    type Shipperz = Shipper[SrcMeta, ElasticsearchPortFactory]

    def bulkFlow: Flow[Shipperz, Option[BulkRequestAndOffsets], NotUsed] = {
      Flow[Shipperz].via(ElasticsearchBulkFlow[SrcMeta](
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

    def makeBulkRequest(bulkRequestAndOffsets: Option[BulkRequestAndOffsets]): Future[Option[BulkResponseAndOffsets]] = {
      val bulkRequestPromise = Promise[Option[BulkResponseAndOffsets]]()

      bulkRequestAndOffsets match {
        case Some(b: BulkRequestAndOffsets) =>
          val (bulkRequest: BulkRequest, offsets: Seq[SrcMeta]) = b

          Future {
            println(s"started bulk of size ${ bulkRequest.requests().size() }")
            elasticsearchClient.bulk(bulkRequest, bulkListener {
              case bulkResponse: BulkResponse =>
                println(s"bulk of size ${ bulkResponse.getItems.length } finished. Time took - ${ bulkResponse.getTookInMillis }" )
                bulkRequestPromise.success(Some((bulkResponse, offsets)))
              case e: Exception =>
                //FIXME: this is error for the bulk request, failure here stops the stream
                println(s"bulk of size ${ bulkRequest.requests().size() } failed" )
                e.printStackTrace()
                //bulkRequestPromise.failure(e)
                bulkRequestPromise.success(None)
            })
          }

        case _ => bulkRequestPromise.success(None)
      }

      bulkRequestPromise.future
    }

    def processBulkResponse(bulkResponseAndOffsets: Option[BulkResponseAndOffsets]): Future[Seq[SrcMeta]] = {
      bulkResponseAndOffsets match {
        case Some(b: BulkResponseAndOffsets) =>
          val (bulkResponse: BulkResponse, offsets: Seq[SrcMeta]) = b

          if (!bulkResponse.hasFailures) {
            //commit offsets directly if no failures at all
            println("bulk request has no errors, committing all offsets")
            var offsetBatch = CommittableOffsetBatch.empty
            offsets foreach { o: SrcMeta =>
              o match {
                case ks: KafkaSourceMetadata => offsetBatch = offsetBatch.updated(ks.offset)
                case _ =>
              }
            }
            offsetBatch.commitScaladsl()
            return Future.successful(offsets)
          }

          // there are some errors, lets find out
          // FIXME: file upload - needs to return error messages
          var offsetBatch = CommittableOffsetBatch.empty
          var finalSeq: Seq[SrcMeta] = Seq()
          bulkResponse.getItems.zip(offsets).foreach { z: (BulkItemResponse, SrcMeta) =>
            val (bulkItemResponse, offset) = z
            if (bulkItemResponse.isFailed) {
              //FIXME: do more detailed failure checking
              println("item " + offset + " errored with " + bulkItemResponse.getFailureMessage)
            } else {
              finalSeq :+= offset
              offset match {
                case k: KafkaSourceMetadata => offsetBatch = offsetBatch.updated(k.offset)
                case _ =>
              }
            }
          }
          offsetBatch.commitScaladsl()
          Future.successful(finalSeq)

        case _ => Future.successful(Seq())
      }
    }

    def sink(onComplete: (Try[Done]) => Unit): Sink[Shipperz, Future[Done]] = {
      if (concurrentRequests < 1) {
        val flow = bulkFlow
          .filter(_.isDefined)
          .mapAsync[Option[BulkResponseAndOffsets]](1)(makeBulkRequest).named("make-bulk-request-mapAsync-single")
          .filter(_.isDefined)
          .mapAsync[Seq[SrcMeta]](1)(processBulkResponse).named("process-bulk-response-mapAsync-single")
          .watchTermination()(Keep.right)

        flow.toMat(Sink.onComplete(onComplete))(Keep.left).named("es-bulk-request-sink-single")
      } else {
        val flow = bulkFlow
          .filter(_.isDefined)
          .mapAsync[Option[BulkResponseAndOffsets]](concurrentRequests)(makeBulkRequest).named("make-bulk-request-mapAsync-parallel")
          .filter(_.isDefined)
          .mapAsync[Seq[SrcMeta]](concurrentRequests)(processBulkResponse).named("process-bulk-response-mapAsync-parallel")
          .watchTermination()(Keep.right)

        flow.toMat(Sink.onComplete(onComplete))(Keep.left).named("es-bulk-request-sink-parallel")
      }
    }
  }

}

