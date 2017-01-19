package com.hoolix.processor.flows

import java.util.concurrent.atomic.AtomicLong

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import com.hoolix.processor.models._
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.common.unit.ByteSizeValue

import scala.collection.JavaConversions

/**
  * Hoolix 2017
  * Created by simon on 1/6/17.
  */
case class ElasticsearchBulkProcessingFlow[SrcMeta <: SourceMetadata](
                                                             bulkActions: Int,
                                                             bulkSize: ByteSizeValue,
                                                             timeoutInMillis: Long
                                                           )
  extends GraphStage[FlowShape[Shipper[SrcMeta, ElasticsearchPortFactory], Option[ElasticsearchBulkRequestContainer[SrcMeta]]]] {

  type Shipperz = Shipper[SrcMeta, ElasticsearchPortFactory]

  val in: Inlet[Shipperz] = Inlet[Shipperz]("EventIn")
  val out: Outlet[Option[ElasticsearchBulkRequestContainer[SrcMeta]]] =
    Outlet[Option[ElasticsearchBulkRequestContainer[SrcMeta]]]("BulkRequestOut")

  val bulkSizeInBytes: Long = bulkSize.getBytes

  override val shape: FlowShape[Shipperz, Option[ElasticsearchBulkRequestContainer[SrcMeta]]] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) {
      val bulkRequestAtomic: AtomicLong = new AtomicLong(0)

      var bulkTime: Long = 0

      var bulkRequestContainer: ElasticsearchBulkRequestContainer[SrcMeta] =
        ElasticsearchBulkRequestContainer(
          bulkRequestAtomic.get(),
          new BulkRequest(),
          bulkResponse = None,
          bulkRequestError = None,
          Seq()
        )

      override def preStart(): Unit = {
        bulkTime = System.currentTimeMillis()
        pull(in)
      }

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          if (isAvailable(in)) {
            val incomingEvent = grab(in)

            val event = incomingEvent.event
            val portFactory = incomingEvent.portFactory
            val sourceMetadata = incomingEvent.sourceMetadata

            bulkRequestContainer = ElasticsearchBulkRequestContainer(
              bulkRequestContainer.id,
              bulkRequestContainer.bulkRequest.add(
                portFactory.generateRequest(sourceMetadata, event).source(
                  JavaConversions.mutableMapAsJavaMap(event.toPayload)
                )
              ),
              bulkRequestContainer.bulkResponse,
              bulkRequestContainer.bulkRequestError,
              bulkRequestContainer.shippers :+ incomingEvent
            )

          }

          val bulkRequest = bulkRequestContainer.bulkRequest
          if (((bulkRequest.estimatedSizeInBytes() < bulkSizeInBytes) &&
            (bulkRequest.numberOfActions() < bulkActions)) &&
            !hasBeenPulled(in)) {
            bulkTime = System.currentTimeMillis()
            pull(in)
          }
        }

        override def onUpstreamFinish(): Unit = {
          if (bulkRequestContainer.bulkRequest.numberOfActions() > 0) {
            println("upstream finished, bulking rest of the events")
            emit(out, Some(bulkRequestContainer))
            complete(out)
          }
        }

        //TODO: what should happen when onUpstreamFailure()?
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          val bulkRequest = bulkRequestContainer.bulkRequest

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

            push(out, Some(bulkRequestContainer))

            bulkRequestContainer = ElasticsearchBulkRequestContainer(
              bulkRequestAtomic.incrementAndGet(),
              new BulkRequest(),
              bulkResponse = None,
              bulkRequestError = None,
              Seq()
            )
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

