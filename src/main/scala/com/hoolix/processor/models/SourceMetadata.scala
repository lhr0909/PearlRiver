package com.hoolix.processor.models

/**
  * Hoolix 2017
  * Created by simon on 1/14/17.
  */
trait SourceMetadata {
  type OffsetT
  val offset: OffsetT
  def id: String
}
