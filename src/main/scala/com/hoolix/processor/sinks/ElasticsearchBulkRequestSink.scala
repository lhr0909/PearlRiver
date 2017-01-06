package com.hoolix.processor.sinks

import akka.NotUsed
import akka.kafka.ConsumerMessage.{CommittableOffset, CommittableOffsetBatch}
import akka.stream.scaladsl.{Flow, Sink}
import com.hoolix.processor.flows.ElasticsearchBulkFlow
import com.hoolix.processor.flows.ElasticsearchBulkFlow.BulkRequestAndOffsets
import com.hoolix.processor.models.KafkaEvent
import com.typesafe.config.Config
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.bulk.{BulkItemResponse, BulkRequest, BulkResponse}
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.unit.{ByteSizeUnit, ByteSizeValue}

import scala.concurrent.{ExecutionContext, Future, Promise}

/**
  * Hoolix 2017
  * Created by simon on 1/1/17.
  */
object ElasticsearchBulkRequestSink {
  type BulkResponseAndOffsets = (BulkResponse, Seq[CommittableOffset])

  def apply(
             elasticsearchClient: TransportClient,
             concurrentRequests: Int
           )(implicit config: Config, ec: ExecutionContext): ElasticsearchBulkRequestSink = {
    val esBulkConfig = config.getConfig("elasticsearch.bulk")
    val maxBulkSizeInBytes = esBulkConfig.getString("max-size-in-bytes")
    val maxBulkActions = esBulkConfig.getInt("max-actions")

    new ElasticsearchBulkRequestSink(elasticsearchClient, maxBulkSizeInBytes, maxBulkActions, concurrentRequests, ec)
  }

  class ElasticsearchBulkRequestSink(
                                      elasticsearchClient: TransportClient,
                                      maxBulkSizeInBytes: String,
                                      maxBulkActions: Int,
                                      concurrentRequests: Int,
                                      implicit val ec: ExecutionContext
                                    ) {

    def bulkFlow: Flow[KafkaEvent, Option[BulkRequestAndOffsets], NotUsed] = {
      Flow[KafkaEvent].via(ElasticsearchBulkFlow(
        maxBulkActions,
        ByteSizeValue.parseBytesSizeValue(
          maxBulkSizeInBytes,
          new ByteSizeValue(5, ByteSizeUnit.MB),
          "elasticsearch.bulk.max-size-in-bytes"
        )
      ))
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
          val (bulkRequest: BulkRequest, offsets: Seq[CommittableOffset]) = b

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

    def processKafkaEvent(bulkResponseAndOffsets: Option[BulkResponseAndOffsets]): Unit = {
      bulkResponseAndOffsets match {
        case Some(b: BulkResponseAndOffsets) =>
          val (bulkResponse: BulkResponse, offsets: Seq[CommittableOffset]) = b

          if (!bulkResponse.hasFailures) {
            //commit offsets directly if no failures at all
            println("bulk request has no errors, committing all offsets")
            offsets.foldLeft(CommittableOffsetBatch.empty)(_.updated(_)).commitScaladsl()
            return
          }

          // there are some errors, lets find out
          var offsetBatch = CommittableOffsetBatch.empty
          bulkResponse.getItems.zip(offsets).foreach { z: (BulkItemResponse, CommittableOffset) =>
            val (bulkItemResponse, offset) = z
            if (bulkItemResponse.isFailed) {
              //FIXME: do more detailed failure checking
              println("item " + offset + " errored with " + bulkItemResponse.getFailureMessage)
            } else {
              offsetBatch = offsetBatch.updated(offset)
            }
          }
          offsetBatch.commitScaladsl()

        case _ =>
      }
    }

    def sink: Sink[KafkaEvent, NotUsed] = {
      if (concurrentRequests < 1) {
        val flow = bulkFlow
          .filter(_.isDefined)
          .mapAsync[Option[BulkResponseAndOffsets]](1)(makeBulkRequest)
          .filter(_.isDefined)

        flow.to(Sink.foreach(processKafkaEvent))
      } else {
        val flow = bulkFlow
          .filter(_.isDefined)
          .mapAsync[Option[BulkResponseAndOffsets]](concurrentRequests)(makeBulkRequest)
          .filter(_.isDefined)

        flow.to(Sink.foreachParallel(concurrentRequests)(processKafkaEvent))
      }
    }
  }

}

