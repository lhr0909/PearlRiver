package com.hoolix.processor.models

/**
  * Created by peiyuchao on 2017/1/18.
  */
case class DummySourceMetadata() extends SourceMetadata {
  // does not know where am I coming from
  override type OffsetT = Any
  override val offset: OffsetT = None
  override def id: String = ???
}
