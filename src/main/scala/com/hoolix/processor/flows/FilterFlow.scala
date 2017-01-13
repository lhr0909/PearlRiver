package com.hoolix.processor.flows

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.hoolix.processor.filters.Filter.ConditionedFilter
import com.hoolix.processor.models.{Event, KafkaTransmitted}
import scala.concurrent.Future

/**
  * Created by peiyuchao on 2017/1/4.
  */
object FilterFlow {
  def apply(
    parallelism: Int
  ): Flow[(KafkaTransmitted, Seq[ConditionedFilter]), KafkaTransmitted, NotUsed] = {

    Flow[(KafkaTransmitted, Seq[ConditionedFilter])].mapAsync(parallelism) { tuple: (KafkaTransmitted, Seq[ConditionedFilter]) =>
      val (kafkaTransmitted, filters) = tuple
      var filtered: Event = kafkaTransmitted.event
      filters.filter(_._1.forall(_(filtered))).foreach((elem) => filtered = elem._2.handle(filtered))
      Future.successful(KafkaTransmitted(kafkaTransmitted.committableOffset, filtered))
    }
  }
}
