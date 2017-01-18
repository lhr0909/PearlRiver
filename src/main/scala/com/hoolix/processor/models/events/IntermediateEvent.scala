package com.hoolix.processor.models.events

/**
  * Created by peiyuchao on 2017/1/5.
  */
case class IntermediateEvent(
                              payload: collection.mutable.Map[String, Any]
                            ) extends Event {
  override def toPayload: collection.mutable.Map[String, Any] = payload

  def _type: String = payload.get("type") match {
    case Some(x: String) => x
    case _ => "_unknown_"
  }

  def token: String = payload.get("token") match {
    case Some(x: String) => x
    case _ => "_unknown_"
  }
}
