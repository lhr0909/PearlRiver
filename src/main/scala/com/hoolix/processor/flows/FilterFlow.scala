package com.hoolix.processor.flows

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.hoolix.processor.filters.Filter
import com.hoolix.processor.models.{Event, KafkaTransmitted}

import scala.concurrent.Future

/**
  * Created by peiyuchao on 2017/1/4.
  */
// TODO 效率可能会比较低，因为每一条新的日志都要查询
case class FilterFlow(parallelism: Int, filters: Map[String, Map[String, Seq[(Seq[(Event) => Boolean], Filter)]]]) {
  def toFlow: Flow[KafkaTransmitted, KafkaTransmitted, NotUsed] = {
    Flow[KafkaTransmitted].mapAsync(parallelism)((kafkaTransmitted: KafkaTransmitted) => {
      var filtered: Event = kafkaTransmitted.event
      val payload = filtered.toPayload
//      val temp : Seq[(Seq[(Event) => Boolean], Filter)] = filters.get(payload("token").asInstanceOf[String]).get(payload("type").asInstanceOf[String])
      val temp : Seq[(Seq[(Event) => Boolean], Filter)] = filters("*")("*") // TODO
      for (elem <- temp) {
        var required = true
        for (r <- elem._1) {
          if (!r(filtered)) required = false
        }
        if (required) filtered = elem._2.handle(filtered)
      }
//      filters.foreach((filter) => filtered = filter.handle(filtered)) // should be sequential
      Future.successful(KafkaTransmitted(kafkaTransmitted.committableOffset, filtered))
    })
  }
}
