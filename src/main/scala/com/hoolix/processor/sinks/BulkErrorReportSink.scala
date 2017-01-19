package com.hoolix.processor.sinks

import akka.Done
import akka.stream.scaladsl.{Flow, Keep, Sink}
import com.hoolix.processor.models.KafkaSourceMetadata

import scala.concurrent.Future

/**
  * Hoolix 2017
  * Created by simon on 1/19/17.
  */
object BulkErrorReportSink {

  //FIXME: currently only reports offsets, need to
  def apply(parallelism: Int): Sink[Option[Seq[KafkaSourceMetadata]], Future[Done]] = {
    Flow[Option[Seq[KafkaSourceMetadata]]].mapAsync(parallelism) {
      case Some(kafkaSourceMetadatas) =>
        println("logging error offsets")
        kafkaSourceMetadatas.foreach(println)
        Future.successful(Some(kafkaSourceMetadatas))
      case _ => Future.successful(None)
    }.watchTermination()(Keep.right).to(Sink.ignore)
  }

}
