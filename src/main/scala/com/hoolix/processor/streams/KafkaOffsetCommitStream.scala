package com.hoolix.processor.streams

import akka.actor.ActorRef
import akka.stream.scaladsl.GraphDSL.Implicits._
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, Merge, RunnableGraph, Sink, Source, SourceQueueWithComplete, ZipWith}
import akka.stream.{ClosedShape, FlowShape, OverflowStrategy}
import com.hoolix.processor.sinks.ElasticsearchBulkRequestSink.AfterBulkTrigger
import com.hoolix.processor.models.KafkaEvent

import scala.collection.immutable
import scala.concurrent.ExecutionContext

/**
  * Hoolix 2017
  * Created by simon on 1/3/17.
  */
object KafkaOffsetCommitStream {
  //                         (bulk source queue,                   bulking source queue,                afterBulk trigger)
  type MaterializedContext = (SourceQueueWithComplete[KafkaEvent], SourceQueueWithComplete[KafkaEvent], ActorRef)
}

case class KafkaOffsetCommitStream(
                                bufferSize: Int,
                                parallelism: Int,
                                implicit val executionContext: ExecutionContext
                                ) {
  def stream: RunnableGraph[KafkaOffsetCommitStream.MaterializedContext] = {
    val sqBulkSourceQueue = Source.queue[KafkaEvent](bufferSize, OverflowStrategy.backpressure)
    val sqBulkingSourceQueue = Source.queue[KafkaEvent](bufferSize, OverflowStrategy.backpressure)
    val afterBulkTriggerSource = Source.actorRef[AfterBulkTrigger](0, OverflowStrategy.dropBuffer)

    val bulkBatch, bulkingBatch = Flow[KafkaEvent].conflateWithSeed(Seq(_))(_ :+ _)

    val sqBulkingConflate = sqBulkingSourceQueue.viaMat(bulkingBatch)(Keep.left)

    val sqBulkConflateFlow = GraphDSL.create(sqBulkSourceQueue) {
      implicit builder =>
        (bulkSQ) =>
          val bulkingQueueInlet = builder.add(Merge[KafkaEvent](2))
          val conflate = builder.add(bulkBatch)

          bulkSQ ~> bulkingQueueInlet ~> conflate

          FlowShape(bulkingQueueInlet.in(1), conflate.out)
    }

    RunnableGraph.fromGraph(
      GraphDSL.create(sqBulkConflateFlow, sqBulkingConflate, afterBulkTriggerSource)((_, _, _)) {
      implicit builder =>
        (sqBC, sqBiC, aBTS) =>
          val zipBulkSource = builder.add(ZipWith((batch: Seq[KafkaEvent], trigger: AfterBulkTrigger) => batch))
          val zipBulkingSource = builder.add(ZipWith((batch: Seq[KafkaEvent], trigger: AfterBulkTrigger) => batch))
          val broadcastSignal = builder.add(Broadcast[AfterBulkTrigger](outputPorts = 2))
          val mapConcatFlow = builder.add(Flow[Seq[KafkaEvent]].mapConcat(_.to[immutable.Seq]))

          //TODO: this sink is sloooow, change to one that uses batch
          val offsetCommitSink = builder.add(
            Flow[Seq[KafkaEvent]]
              .mapConcat(_.to[immutable.Seq])
              .to(Sink.foreachParallel(parallelism)(_.getCommittableOffset.commitScaladsl()))
          )

                             sqBiC ~> zipBulkingSource.in0
          aBTS  ~> broadcastSignal ~> zipBulkingSource.in1
                                      zipBulkingSource.out ~> mapConcatFlow ~> sqBC ~> zipBulkSource.in0
                   broadcastSignal ~>                                                  zipBulkSource.in1
                                                                                       zipBulkSource.out ~> offsetCommitSink

          ClosedShape
      }
    )
  }
}
