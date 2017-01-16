package com.hoolix.processor.models.events

/**
  * Created by peiyuchao on 2017/1/6.
  */
case class LineEvent(message: String) extends Event {
  override def toPayload = collection.mutable.Map("message" -> message)
}
