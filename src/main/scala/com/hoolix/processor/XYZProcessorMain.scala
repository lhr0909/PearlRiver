package com.hoolix.processor

import java.io.File
import java.time.Instant
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.hoolix.processor.modules.{ElasticsearchClient, KafkaConsumerSettings}
import com.hoolix.processor.streams.KafkaToEsStream
import com.typesafe.config.ConfigFactory

import scala.concurrent.Await
import scala.concurrent.duration._


/**
  * Hoolix 2016
  * Created by simon on 12/29/16.
  */
object XYZProcessorMain extends App {

  override def main(args: Array[String]): Unit = {
    implicit val config = ConfigFactory.parseFile(new File("conf/application.conf"))

    implicit val system = ActorSystem("xyz-processor", config)
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatchers.lookup("xyz-dispatcher")

    val stream = KafkaToEsStream(
      parallelism = 5,
      maxSize = 100000,
      ElasticsearchClient(),
      KafkaConsumerSettings(),
      Set("hooli_topic"),
      executionContext
    )

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
