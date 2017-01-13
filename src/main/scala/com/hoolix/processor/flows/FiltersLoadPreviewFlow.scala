package com.hoolix.processor.flows

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.hoolix.processor.filters.Filter.ConditionedFilter
import com.hoolix.processor.models.Event
import scala.concurrent.Future

/**
  * Created by peiyuchao on 2017/1/13.
  */
object FiltersLoadPreviewFlow {
  def apply(parallelism: Int, filters: Seq[ConditionedFilter]): Flow[Event, (Event, Seq[ConditionedFilter]), NotUsed] = {
    println("enter FiltersLoadPreviewFlow")
    Flow[Event].mapAsync(parallelism) { event: Event =>
      Future.successful((event, filters))
    }
  }
}
