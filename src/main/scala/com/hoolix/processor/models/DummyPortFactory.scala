package com.hoolix.processor.models

import com.hoolix.processor.models.events.Event

/**
  * Created by peiyuchao on 2017/1/18.
  */
case class DummyPortFactory() extends PortFactory {
  // does not know where am I going to
  override def generateRequest(sourceMetadata: SourceMetadata, event: Event): T = ???
}
