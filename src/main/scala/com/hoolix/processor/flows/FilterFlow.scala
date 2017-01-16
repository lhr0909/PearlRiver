package com.hoolix.processor.flows

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.hoolix.processor.filters.Filter.ConditionedFilter
import com.hoolix.processor.filters.Filter
import com.hoolix.processor.models.{PortFactory, Shipper, SourceMetadata}
import com.hoolix.processor.models.events.Event
import scala.concurrent.Future

/**
  * Created by peiyuchao on 2017/1/4.
  */
object FilterFlow {
  def apply(
    parallelism: Int
  ): Flow[(KafkaTransmitted, Seq[ConditionedFilter]), KafkaTransmitted, NotUsed] = {

    Flow[(KafkaTransmitted, Seq[ConditionedFilter])].mapAsync(parallelism) { tuple: (KafkaTransmitted, Seq[ConditionedFilter]) =>
      val (kafkaTransmitted, filters) = tuple
      var filtered: Event = kafkaTransmitted.event
      filters.filter(_._1.forall(_ (filtered))).foreach((elem) => filtered = elem._2.handle(filtered))
      Future.successful(KafkaTransmitted(kafkaTransmitted.committableOffset, filtered))
    }
  }
}

case class FilterFlow[SrcMeta <: SourceMetadata, PortFac <: PortFactory](
                       parallelism: Int,
                       filters: Map[String, Map[String, Seq[ConditionedFilter]]]
                     ) {

  type Shipperz = Shipper[SrcMeta, PortFac]

  // TODO 效率可能会比较低，因为每一条新的日志都要查询
  def flow(): Flow[Shipper[SrcMeta, PortFac], Shipper[SrcMeta, PortFac], NotUsed] = {

    Flow[Shipperz].mapAsync(parallelism) { incomingEvent: Shipperz =>
      var filtered: Event = incomingEvent.event
      val payload = filtered.toPayload
      //      filters.getOrElse(payload("token").asInstanceOf[String], filters("*"))

      val eventType = payload.get("type") match {
        case Some(t) => t.asInstanceOf[String]
        case _ => "_unknown_"
      }

      if (!filters("*").contains(eventType)) {
        // if there is no filter rules available, return current event directly
        println(s"could not find matching parsing rule for type $eventType, sending raw event")
        Future.successful(incomingEvent)
      } else {
        val temp: Seq[ConditionedFilter] = filters("*")(eventType)
        //TODO 此处应有reduce和map
        for (elem <- temp) {
          var required = true
          for (r <- elem._1) {
            if (!r(filtered)) required = false
          }
          if (required) filtered = elem._2.handle(filtered)
        }
        Future.successful(
          Shipper(
            filtered,
            incomingEvent.sourceMetadata,
            incomingEvent.portFactory
          )
        )
      }
    }.named("filter-flow")

  }
}
