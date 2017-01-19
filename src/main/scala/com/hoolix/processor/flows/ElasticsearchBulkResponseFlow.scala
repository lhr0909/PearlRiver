package com.hoolix.processor.flows

import akka.NotUsed
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL}
import akka.stream.{FanOutShape3, Graph}
import com.hoolix.processor.models.{ElasticsearchBulkRequestContainer, ElasticsearchPortFactory, Shipper, SourceMetadata}
import org.elasticsearch.action.bulk.BulkItemResponse

import scala.concurrent.Future

/**
  * Hoolix 2017
  * Created by simon on 1/18/17.
  */
object ElasticsearchBulkResponseFlow {

  //FIXME: now we are sending only the source metadata, should maybe send the whole shipper seq
  def apply[SrcMeta <: SourceMetadata](
                                      parallelism: Int
                                      ):
    Graph[FanOutShape3[Option[ElasticsearchBulkRequestContainer[SrcMeta]], Option[ElasticsearchBulkRequestContainer[SrcMeta]], Option[Seq[SrcMeta]], Option[Seq[SrcMeta]]], NotUsed] =
      GraphDSL.create() {
        implicit builder: GraphDSL.Builder[NotUsed] =>
          import GraphDSL.Implicits._

          val checkRequestErrorFlow = builder.add(Flow[Option[ElasticsearchBulkRequestContainer[SrcMeta]]].mapAsync(parallelism) {
            case Some(container) =>
              container.bulkRequestError match {
                case Some(_) => Future.successful(Some(container))
                case _ => Future.successful(None)
              }
            case _ => Future.successful(None)
          })

          val checkResponseSuccessFlow = builder.add(Flow[Option[ElasticsearchBulkRequestContainer[SrcMeta]]].mapAsync[Option[Seq[SrcMeta]]](parallelism) {
            case Some(container) =>
              container.bulkResponse match {
                case Some(response) =>
                  if (!response.hasFailures) {
                    println(s"Bulk Request #${ container.id } has no failures, sending all offsets to commit")
                    Future.successful(Some(container.shippers.map[SrcMeta](_.sourceMetadata).toSeq))
                  } else {
                    val successSrcMeta = response.getItems.zip(container.shippers).filter {
                      x: ((BulkItemResponse, Shipper[SrcMeta, ElasticsearchPortFactory])) =>
                        val (responseItem, _) = x
                        !responseItem.isFailed
                    }.map(_._2.sourceMetadata).toSeq
                    Future.successful(Some(successSrcMeta))
                  }
                case _ => Future.successful(None)
              }
            case _ => Future.successful(None)
          })

          val checkResponseFailureFlow = builder.add(Flow[Option[ElasticsearchBulkRequestContainer[SrcMeta]]].mapAsync[Option[Seq[SrcMeta]]](parallelism) {
            case Some(container) =>
              container.bulkResponse match {
                case Some(response) =>
                  if (!response.hasFailures) {
                    Future.successful(None)
                  } else {
                    println(s"Bulk Request #${ container.id } has failures, picking out failed items")
                    val failedSrcMeta = response.getItems.zip(container.shippers).filter {
                      x: ((BulkItemResponse, Shipper[SrcMeta, ElasticsearchPortFactory])) =>
                        val (responseItem, shipper) = x
                        if (responseItem.isFailed) {
                          println(s"${ shipper.sourceMetadata.offset.toString } has failure - ${ responseItem.getFailureMessage }")
                          true
                        } else { false }
                    }.map(_._2.sourceMetadata).toSeq
                    Future.successful(Some(failedSrcMeta))
                  }
                case _ => Future.successful(None)
              }
            case _ => Future.successful(None)
          })

          val broadcastFlow = builder.add(Broadcast[Option[ElasticsearchBulkRequestContainer[SrcMeta]]](3))
          val filterErrorContainer = builder.add(Flow[Option[ElasticsearchBulkRequestContainer[SrcMeta]]].filter(_.isDefined))
          val filterSourceMetaSuccess, filterSourceMetaFalure = builder.add(Flow[Option[Seq[SrcMeta]]].filter(_.isDefined))

          broadcastFlow.out(0) ~> checkRequestErrorFlow ~> filterErrorContainer
          broadcastFlow.out(1) ~> checkResponseSuccessFlow ~> filterSourceMetaSuccess
          broadcastFlow.out(2) ~> checkResponseFailureFlow ~> filterSourceMetaFalure

          new FanOutShape3[Option[ElasticsearchBulkRequestContainer[SrcMeta]], Option[ElasticsearchBulkRequestContainer[SrcMeta]], Option[Seq[SrcMeta]], Option[Seq[SrcMeta]]](
            broadcastFlow.in,
            filterErrorContainer.out,
            filterSourceMetaSuccess.out,
            filterSourceMetaFalure.out
          )
      }


}
