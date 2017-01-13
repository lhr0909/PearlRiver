package com.hoolix.processor.flows

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.hoolix.processor.filters.Filter.ConditionedFilter
import com.hoolix.processor.models.Event
import scala.concurrent.Future

/**
  * Created by peiyuchao on 2017/1/13.
  */
object FilterPreviewFlow {
  def apply(
    parallelism: Int
  ): Flow[(Event, Seq[ConditionedFilter]), Event, NotUsed] = {

    Flow[(Event, Seq[ConditionedFilter])].mapAsync(parallelism) { tuple: (Event, Seq[ConditionedFilter]) =>
      val (event, filters) = tuple
      var filtered: Event = event
      filters.filter(_._1.forall(_(filtered))).foreach((elem) => filtered = elem._2.handle_preview(filtered))
      Future.successful(filtered)
    }
  }
}
