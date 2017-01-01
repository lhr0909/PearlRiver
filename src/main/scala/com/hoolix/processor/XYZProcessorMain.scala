package com.hoolix.processor

import java.net.InetAddress
import java.time.Instant
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.Sink
import com.hoolix.processor.models._
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.elasticsearch.action.bulk.{BackoffPolicy, BulkProcessor, BulkRequest, BulkResponse}
import org.elasticsearch.action.bulk.BulkProcessor.Listener
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.unit.{ByteSizeUnit, ByteSizeValue, TimeValue}
import org.elasticsearch.transport.client.PreBuiltTransportClient

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._


/**
  * Hoolix 2016
  * Created by simon on 12/29/16.
  */
object XYZProcessorMain extends App {

  override def main(args: Array[String]): Unit = {
    val MAX_SIZE = 10000
    //TODO: figure out how to load config from outside of resources folder
    val config = ConfigFactory.load

    implicit val system = ActorSystem("xyz-processor", config)
    implicit val materializer = ActorMaterializer()

    implicit val executionContext = system.dispatchers.lookup("xyz-dispatcher")

    val esClient = new PreBuiltTransportClient(Settings.EMPTY)
      .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("0.0.0.0"), 9300))
    val bulkProcessor = BulkProcessor.builder(
      esClient,
      new Listener {
        override def beforeBulk(executionId: Long, request: BulkRequest) = {

        }

        override def afterBulk(executionId: Long, request: BulkRequest, response: BulkResponse) = {
          println("bulk result")
          println("bulk size - " + response.getItems.length)
          println("bulk time - " + response.getTookInMillis)
          println("end bulk result")
        }

        override def afterBulk(executionId: Long, request: BulkRequest, failure: Throwable) = {
          throw failure
        }
      }
    )
      .setBulkActions(MAX_SIZE)
      .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
//      .setFlushInterval(TimeValue.timeValueSeconds(5))
      .setConcurrentRequests(5)
      .setBackoffPolicy(
        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3)
      )
      .build()

    //TODO: put all consumer settings in application.conf
    val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer)
      .withBootstrapServers("0.0.0.0:9092")
      .withGroupId("xyz-processor")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val done = Consumer.committableSource(consumerSettings, Subscriptions.topics("hooli_topic"))
        .buffer(MAX_SIZE, OverflowStrategy.backpressure)
        .mapAsync[KafkaFilebeatEvent](10) { committableMessage: CommittableMessage[Array[Byte], String] =>
          Future.successful(KafkaFilebeatEvent(
            FilebeatEvent.fromJsonString(committableMessage.record.value),
            committableMessage.committableOffset
          ))
        }
        .batch(max = MAX_SIZE, first => Seq(first)) { (batch, elem) => batch :+ elem }
        .runWith(Sink.foreachParallel(5) { events: Seq[KafkaFilebeatEvent] =>
          println("batch size - " + events.length)
          events.map { event =>
            bulkProcessor.add(event.filebeatEvent.toIndexRequest.source(event.filebeatEvent.toEsPayload))
          }
        })

    //TODO: improve logging
    scala.sys.addShutdownHook {
      println("Terminating... - " + Instant.now)
      bulkProcessor.awaitClose(30, TimeUnit.SECONDS)
      system.terminate()
      Await.result(system.whenTerminated, 30 seconds)
      println("Terminated... Bye - " + Instant.now)
    }
  }
}
