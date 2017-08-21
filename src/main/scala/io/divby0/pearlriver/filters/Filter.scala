package io.divby0.pearlriver.filters

import io.divby0.pearlriver.models.Event

/**
  * Hoolix 2017
  * Created by peiyuchao on 2017/1/3.
  */
trait Filter {
  def handle(event: Event): Event
}
