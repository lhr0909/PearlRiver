package com.hoolix.processor

import com.hoolix.processor.models.builders.KafkaEventBuilder
import com.hoolix.processor.models.{FileBeatEvent, KafkaEvent}

import scala.concurrent.Future

/**
  * Hoolix 2017
  * Created by peiyuchao on 1/3/17.
  */
trait ConfigLoader {
  // load configurations,
  def load(): Seq[Filter]


}
