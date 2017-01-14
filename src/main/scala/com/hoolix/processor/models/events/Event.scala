package com.hoolix.processor.models.events

/**
  * Created by peiyuchao on 2017/1/5.
  */
trait Event {
  def toPayload: collection.mutable.Map[String, Any]
}
