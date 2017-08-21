package io.divby0.pearlriver.models

/**
  * Created by peiyuchao on 2017/1/5.
  */
trait Event extends ElasticSearchSinkAble {
  def toPayload: collection.mutable.Map[String, Any]
}
