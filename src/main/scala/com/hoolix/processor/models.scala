package com.hoolix.processor

import akka.kafka.ConsumerMessage.CommittableOffset

/**
  * Hoolix 2016
  * Created by simon on 12/31/16.
  */
package object models {
  case class KafkaFilebeatEvent(
                               filebeatEvent: FilebeatEvent,
                               kafkaOffset: CommittableOffset
                               )
}
