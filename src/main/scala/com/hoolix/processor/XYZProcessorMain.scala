package com.hoolix.processor

import java.io.File
import java.time.Instant
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.kafka.scaladsl.Consumer.Control
import akka.stream.{ActorMaterializer, KillSwitch}
import com.hoolix.processor.http.routes.OfflineQueryRoutes
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

    val esClient = ElasticsearchClient()

    var kafkaControl: Control = null

    val stream = KafkaToEsStream(
      parallelism = 5,
      esClient,
      Set("hooli_topic")
    )

    val httpConfig = config.getConfig("http")

    val route: Route = pathSingleSlash {
      complete("后端程序还活着！")
    } ~ OfflineQueryRoutes() ~
    path("start") {
//      val (esBulkProcessor, kafkaControl) = stream.run()
      kafkaControl = stream.run()
      complete("pipeline started")
    } ~
    path("stop") {
      println(s"Shutting down Kafka Source now... - " + Instant.now)
      kafkaControl match {
        case a: Control =>
          //TODO: need to figure out a mechanism to wait on shutdown, otherwise it is better to use BulkProcessor
          onSuccess(a.stop()) { extraction =>
            complete("done")
          }
        case _ => complete("no stream started, but ok")
      }
    }


    val bindAddress = httpConfig.getString("bind-address")
    val bindPort = httpConfig.getInt("bind-port")
    val httpBind = Http().bindAndHandle(route, bindAddress, bindPort)

    httpBind.onComplete { _ =>
      println(s"HTTP Server started at $bindAddress:$bindPort !")
    }

    //TODO: improve logging (use log4j2)
    scala.sys.addShutdownHook {
      val terminateSeconds = 120
      println(s"Shutting down HTTP service in $terminateSeconds seconds..." + Instant.now)
      httpBind
        .flatMap(_.unbind()) // trigger unbinding from the port
        .onComplete { _ =>
          println(s"Waiting for Akka Actor System to shut down in $terminateSeconds seconds... - " + Instant.now)
          println(s"Shutting down Akka Actor System now - " + Instant.now)
          system.terminate()
          Await.result(system.whenTerminated, terminateSeconds.seconds)
          println("Terminated safely. Cheers - " + Instant.now)
      }
    }
  }
}
