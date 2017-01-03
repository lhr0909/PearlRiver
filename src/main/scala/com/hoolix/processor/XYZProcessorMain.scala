package com.hoolix.processor

import java.io.File
import java.net.InetAddress
import java.time.Instant
import java.util.concurrent.TimeUnit

import akka.actor.{ActorSystem, Props}
import akka.kafka.ConsumerSettings
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Keep
import com.hoolix.processor.sinks.ElasticsearchBulkRequestSink
import com.hoolix.processor.sources.KafkaSource
import com.hoolix.processor.streams.KafkaOffsetCommitStream
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

import scala.concurrent.Await
import scala.concurrent.duration._


/**
  * Hoolix 2016
  * Created by simon on 12/29/16.
  */
object XYZProcessorMain extends App {

  override def main(args: Array[String]): Unit = {
    val config = ConfigFactory.parseFile(new File("conf/application.conf"))

    implicit val system = ActorSystem("xyz-processor", config)
    implicit val materializer = ActorMaterializer()

    implicit val executionContext = system.dispatchers.lookup("xyz-dispatcher")

    //TODO: es client settings in application.conf
    val esClient = new PreBuiltTransportClient(Settings.EMPTY)
      .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("0.0.0.0"), 9300))


    //TODO: kafka consumer settings in application.conf
    val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer)
      .withBootstrapServers("0.0.0.0:9092")
      .withGroupId("xyz-processor")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    val kafkaSource = KafkaSource(20, consumerSettings, "hooli_topic")

    // main stream
    val maxBulkSize = 100000

    val kafkaOffsetCommitStreamContext = KafkaOffsetCommitStream(
      bufferSize = maxBulkSize,
      parallelism = 3,
      executionContext
    ).stream.run()

    val esSink = ElasticsearchBulkRequestSink(
      esClient,
      maxBulkSize = 100000,
      kafkaOffsetCommitStreamContext,
      executionContext
    )

    val (mainStream, control) = kafkaSource.source.toMat(esSink.sink)(Keep.both).run()

    //TODO: improve logging
    scala.sys.addShutdownHook {
      println("Terminating... - " + Instant.now)
      esSink.bulkProcessor.awaitClose(30, TimeUnit.SECONDS)
      system.terminate()
      Await.result(system.whenTerminated, 30 seconds)
      println("Terminated... Bye - " + Instant.now)
    }
  }
}
