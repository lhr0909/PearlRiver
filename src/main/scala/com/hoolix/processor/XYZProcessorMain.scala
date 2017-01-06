package com.hoolix.processor

import java.io.File
import java.net.InetAddress
import java.time.Instant
import java.util.concurrent.TimeUnit

import org.slf4j.LoggerFactory
import akka.actor.ActorSystem
import akka.kafka.ConsumerSettings
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.hoolix.processor.decoders.FileBeatDecoder
import com.hoolix.processor.filters.Filter
import com.hoolix.processor.flows.{DecodeFlow, FilterFlow}
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.hoolix.processor.sinks.ElasticsearchBulkRequestSink
import com.hoolix.processor.sources.KafkaSource
import com.hoolix.processor.streams.KafkaToEsStream
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.elasticsearch.common.logging.Loggers
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
  val logger = LoggerFactory.getLogger(this.getClass)

  override def main(args: Array[String]): Unit = {
    val config = ConfigFactory.parseFile(new File("conf/application.conf"))


    val decider: Supervision.Decider = { e =>
      logger.error("Unhandled exception in stream", e)
      e.printStackTrace()
//      logger.error(e.printStackTrace())
      Supervision.Stop
    }




    implicit val system = ActorSystem("xyz-processor", config)
    val materializerSettings = ActorMaterializerSettings(system).withSupervisionStrategy(decider)
//    implicit val materializer = ActorMaterializer()
    implicit val materializer = ActorMaterializer(materializerSettings)(system)

    implicit val executionContext = system.dispatchers.lookup("xyz-dispatcher")

    //TODO: es client settings in application.conf
    val esClient = new PreBuiltTransportClient(Settings.EMPTY)
      .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("0.0.0.0"), 9300))


    //TODO: kafka consumer settings in application.conf
    val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer)
      .withBootstrapServers("0.0.0.0:9092")
      .withGroupId("xyz-processor")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")




    val kafkaTopics = Set("test_topic3")
    val stream = KafkaToEsStream(
      parallelism = 1,
      maxSize = 2,
      esClient,
      consumerSettings,
      kafkaTopics,
      executionContext
    )

//    val source = Source(0 to 5).map(100 / _)
//    val result = source.runWith(Sink.fold(0)(_ + _))
//    println(result)

    val (esBulkProcessor, kafkaControl) = stream.run()


    //TODO: improve logging (use log4j2)
    scala.sys.addShutdownHook {
      val terminateSeconds = 30
      println(s"Shutting down ES Bulk Processor in $terminateSeconds seconds... - " + Instant.now)
      esBulkProcessor.awaitClose(terminateSeconds, TimeUnit.SECONDS)
      println(s"Shutting down Kafka Source in $terminateSeconds seconds... - " + Instant.now)
      Await.result(kafkaControl.shutdown, terminateSeconds.seconds)
      println(s"Shutting down Akka Actor System in $terminateSeconds seconds... - " + Instant.now)
      system.terminate()
      Await.result(system.whenTerminated, terminateSeconds.seconds)
      println("Terminated safely. Cheers - " + Instant.now)
    }
  }
}
