package com.hoolix.processor.flows

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.hoolix.processor.filters.Filter
import com.hoolix.processor.models.{Event, KafkaTransmitted}

import scala.concurrent.Future

/**
  * Created by peiyuchao on 2017/1/4.
  */
case class FilterFlow(parallelism: Int, filters: Seq[Filter]) {
  def toFlow: Flow[KafkaTransmitted, KafkaTransmitted, NotUsed] = {
    Flow[KafkaTransmitted].mapAsync(parallelism)((kafkaTransmitted: KafkaTransmitted) => {
      var filtered: Event = kafkaTransmitted.event
      filters.foreach((filter) => filtered = filter.handle(filtered)) // should be sequential
      Future.successful(KafkaTransmitted(kafkaTransmitted.committableOffset, filtered))
    })
  }
}
