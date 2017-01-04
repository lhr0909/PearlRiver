package com.hoolix.processor.filters

import com.hoolix.processor.models.Event

/**
  * Hoolix 2017
  * Created by peiyuchao on 2017/1/3.
  */
trait Filter {
  def handle(event: Event): Event
}
