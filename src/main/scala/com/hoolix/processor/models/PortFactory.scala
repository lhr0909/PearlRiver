package com.hoolix.processor.models

import com.hoolix.processor.models.events.Event

/**
  * Hoolix 2017
  * Created by simon on 1/14/17.
  */
trait PortFactory {
  type T
  def generateRequest(sourceMetadata: SourceMetadata, event: Event): T
}
