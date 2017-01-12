package com.hoolix.processor.flows

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.hoolix.processor.filters.Filter
import com.hoolix.processor.flows.FilterFlow.FilterMatchingRule
import com.hoolix.processor.models.{Event, KafkaTransmitted}

import scala.concurrent.Future

/**
  * Created by peiyuchao on 2017/1/4.
  */
object FilterFlow {
  type EventFilterPredicate = (Event) => Boolean
  type FilterMatchingRule = (Seq[EventFilterPredicate], Filter)

  // TODO 效率可能会比较低，因为每一条新的日志都要查询
  def apply(
             parallelism: Int,
             filters: Map[String, Map[String, Seq[FilterMatchingRule]]]
           ): Flow[KafkaTransmitted, KafkaTransmitted, NotUsed] = {

    Flow[KafkaTransmitted].mapAsync(parallelism) { incomingEvent: KafkaTransmitted =>
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
        val temp: Seq[FilterMatchingRule] = filters("*")(eventType)
        //TODO 此处应有reduce和map
        for (elem <- temp) {
          var required = true
          for (r <- elem._1) {
            if (!r(filtered)) required = false
          }
          if (required) filtered = elem._2.handle(filtered)
        }
        Future.successful(KafkaTransmitted(incomingEvent.committableOffset, filtered))
      }
    }.named("filter-flow")

  }
}
