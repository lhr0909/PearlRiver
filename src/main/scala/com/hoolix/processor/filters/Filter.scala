package com.hoolix.processor.filters

import com.hoolix.processor.models.events.Event

/**
  * Hoolix 2017
  * Created by peiyuchao on 2017/1/3.
  */
trait Filter {
  def handle(event: Event): Event
  def handle_preview(event: Event): Event = ???
}

object Filter {
  type FilterPrecondition = (Event) => Boolean
  type ConditionedFilter = (Seq[FilterPrecondition], Filter)
}
