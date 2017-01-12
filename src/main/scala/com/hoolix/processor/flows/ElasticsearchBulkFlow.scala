package com.hoolix.processor.flows

import akka.kafka.ConsumerMessage.CommittableOffset
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import com.hoolix.processor.models.KafkaTransmitted
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.common.unit.ByteSizeValue

import scala.collection.JavaConversions

/**
  * Hoolix 2017
  * Created by simon on 1/6/17.
  */
object ElasticsearchBulkFlow {
  type BulkRequestAndOffsets = (BulkRequest, Seq[CommittableOffset])

  def apply(
           bulkActions: Int,
           bulkSize: ByteSizeValue,
           timeoutInMillis: Long
           ): GraphStage[FlowShape[KafkaTransmitted, Option[BulkRequestAndOffsets]]] =
    new ElasticsearchBulkFlow(bulkActions, bulkSize, timeoutInMillis)

  class ElasticsearchBulkFlow(
                               bulkActions: Int,
                               bulkSize: ByteSizeValue,
                               timeoutInMillis: Long
                             ) extends GraphStage[FlowShape[KafkaTransmitted, Option[BulkRequestAndOffsets]]] {

    val in: Inlet[KafkaTransmitted] = Inlet[KafkaTransmitted]("EventIn")
    val out: Outlet[Option[BulkRequestAndOffsets]] = Outlet[Option[BulkRequestAndOffsets]]("BulkRequestOut")

    val bulkSizeInBytes: Long = bulkSize.getBytes

    var bulkRequest: BulkRequest = new BulkRequest()
    var offsets: Seq[CommittableOffset] = Seq()
    var bulkTime: Long = 0

    override val shape: FlowShape[KafkaTransmitted, Option[BulkRequestAndOffsets]] = FlowShape(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
      new GraphStageLogic(shape) {
        override def preStart(): Unit = {
          bulkTime = System.currentTimeMillis()
          pull(in)
        }

        setHandler(in, new InHandler {
          override def onPush(): Unit = {
            if (isAvailable(in)) {
              val incomingEvent = grab(in)

              bulkRequest = bulkRequest.add(
                incomingEvent.toIndexRequest.source(
                  JavaConversions.mutableMapAsJavaMap(incomingEvent.event.toPayload)
                )
              )

              offsets :+= incomingEvent.committableOffset
            }

            if (((bulkRequest.estimatedSizeInBytes() < bulkSizeInBytes) &&
              (bulkRequest.numberOfActions() < bulkActions)) &&
              !hasBeenPulled(in)) {
              bulkTime = System.currentTimeMillis()
              pull(in)
            }
          }

          override def onUpstreamFinish(): Unit = {
            if (bulkRequest.numberOfActions() > 0) {
              println("upstream finished, bulking rest of the events")
              emit(out, Some(bulkRequest, offsets))
              complete(out)
            }
          }

          //TODO: what should happen when onUpstreamFailure()?
        })

        setHandler(out, new OutHandler {
          override def onPull(): Unit = {
            if (isClosed(out)) {
              if (bulkRequest.numberOfActions() > 0) {
                fail(out, new IllegalStateException("there are still requests left to be bulked!"))
              } else {
                complete(out)
              }
              return
            }

            // if it reached maxSize or maxActions, or the timeout has hit, a push is triggered
            if (((bulkRequest.estimatedSizeInBytes() >= bulkSizeInBytes) ||
                 (bulkRequest.numberOfActions() >= bulkActions) ||
                 ((System.currentTimeMillis() - bulkTime >= timeoutInMillis) &&
                  (bulkRequest.numberOfActions() > 0))) &&
                isAvailable(out)) {

              push(out, Some(bulkRequest, offsets))
              bulkRequest = new BulkRequest()
              offsets = Seq()
              bulkTime = System.currentTimeMillis()

              if (!hasBeenPulled(in)) {
                pull(in)
              }
            } else {
              push(out, None)
            }
          }

          //TODO: what should happen when onDownstreamFinish()?
        })
      }
    }
  }
}

