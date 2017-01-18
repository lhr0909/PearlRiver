package com.hoolix.processor.sinks

import akka.Done
import akka.stream.scaladsl.{Flow, Keep, Sink}
import com.hoolix.processor.models.{ElasticsearchBulkRequestContainer, KafkaSourceMetadata}
import java.util

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

import scala.concurrent.Future

/**
  * Hoolix 2017
  * Created by simon on 1/18/17.
  */
object KafkaOffsetCommitSink {

  def apply(
             parallelism: Int,
             reactiveKafkaSink: ReactiveKafkaSink
           ): Sink[Option[Seq[KafkaSourceMetadata]], Future[Done]] = {

    Flow[Option[Seq[KafkaSourceMetadata]]].mapAsync[util.Map[TopicPartition, OffsetAndMetadata]](parallelism) {
      case Some(srcMetas) =>
        val finalMap = new util.HashMap[TopicPartition, OffsetAndMetadata]()

        srcMetas foreach { srcMeta =>
          //TODO: put meaningful metadata for extra record
          finalMap.put(srcMeta.topicPartition, srcMeta.offsetAndMetadata("xyz-processor"))
        }

        Future.successful(finalMap)
      case _ => Future.successful(new util.HashMap[TopicPartition, OffsetAndMetadata]())
    }
  }.toMat(Sink.fromGraph(reactiveKafkaSink))(Keep.right)

}
