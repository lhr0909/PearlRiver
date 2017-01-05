package com.hoolix.processor.flows

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.hoolix.processor.filters.Filter
import com.hoolix.processor.models.Event

import scala.concurrent.Future

/**
  * Created by peiyuchao on 2017/1/4.
  */
case class FilterFlow(parallelism: Int, filters: Seq[Filter]) {
  def toFlow: Flow[Event, Event, NotUsed] = {
    Flow[Event].mapAsync(parallelism)((event: Event) => {
      var filtered: Event = event
      filters.foreach((filter) => filtered = filter.handle(filtered)) // should be sequential
      Future.successful(filtered)
    })
  }
}
