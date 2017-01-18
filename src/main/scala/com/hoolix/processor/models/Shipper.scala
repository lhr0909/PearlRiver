package com.hoolix.processor.models

import com.hoolix.processor.models.events.Event

/**
  * Hoolix 2017
  * Created by simon on 1/13/17.
  */
case class Shipper[SrcMeta <: SourceMetadata, PortFac <: PortFactory](
                                                                       event: Event,
                                                                       sourceMetadata: SrcMeta,
                                                                       portFactory: PortFac
                                                                     ) {}
