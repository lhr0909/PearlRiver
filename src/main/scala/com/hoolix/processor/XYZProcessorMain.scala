package com.hoolix.processor

import java.net.InetAddress
import java.time.Instant
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.kafka.ConsumerSettings
import akka.stream.ActorMaterializer
import com.hoolix.processor.pipelines.sinks.ElasticsearchBulkRequestSink
import com.hoolix.processor.pipelines.sources.KafkaSource
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
    //TODO: figure out how to load config from outside of resources folder
    val config = ConfigFactory.load

    implicit val system = ActorSystem("xyz-processor", config)
    implicit val materializer = ActorMaterializer()

    implicit val executionContext = system.dispatchers.lookup("xyz-dispatcher")

    //TODO: es client settings in application.conf
    val esClient = new PreBuiltTransportClient(Settings.EMPTY)
      .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("0.0.0.0"), 9300))
    val esSink = ElasticsearchBulkRequestSink(esClient, -1, 0, executionContext)

    //TODO: put all consumer settings in application.conf
    val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer)
      .withBootstrapServers("0.0.0.0:9092")
      .withGroupId("xyz-processor")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    val kafkaSource = KafkaSource(10, consumerSettings, "hooli_topic")

    // main stream
    val mainStream = kafkaSource.toSource.runWith(esSink.toSink)

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
