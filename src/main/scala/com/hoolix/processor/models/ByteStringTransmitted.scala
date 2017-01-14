package com.hoolix.processor.models

import com.hoolix.processor.models.events.Event

/**
  * Hoolix 2017
  * Created by simon on 1/13/17.
  */
case class ByteStringTransmitted(offset: FileOffset, event: Event) {}
