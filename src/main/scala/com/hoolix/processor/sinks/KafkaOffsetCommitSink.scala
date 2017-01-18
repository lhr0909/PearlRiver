package com.hoolix.processor.sinks

import akka.Done
import akka.stream.scaladsl.Sink
import com.hoolix.processor.models.{ElasticsearchBulkRequestContainer, KafkaSourceMetadata}

import scala.concurrent.Future

/**
  * Hoolix 2017
  * Created by simon on 1/18/17.
  */
object KafkaOffsetCommitSink {

  def apply(reactiveKafkaSink: ReactiveKafkaSink): Sink[, Future[Done]] = {

  }

}
