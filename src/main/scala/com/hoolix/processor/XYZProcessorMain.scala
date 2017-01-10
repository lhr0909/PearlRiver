package com.hoolix.processor

import java.io.File
import java.time.Instant

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.hoolix.processor.http.routes.{OfflineQueryRoutes, StreamControlRoutes}
import com.hoolix.processor.modules.ElasticsearchClient
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration._


/**
  * Hoolix 2016
  * Created by simon on 12/29/16.
  */
object XYZProcessorMain extends App {
  val logger = LoggerFactory.getLogger(this.getClass)

  override def main(args: Array[String]): Unit = {
    implicit val config = ConfigFactory.parseFile(new File("conf/application.conf"))

    val decider: Supervision.Decider = { e =>
      logger.error("Unhandled exception in stream", e)
      e.printStackTrace()
      Supervision.Stop
    }

    implicit val system = ActorSystem("xyz-processor", config)
    val materializerSettings = ActorMaterializerSettings(system).withSupervisionStrategy(decider)
    implicit val materializer = ActorMaterializer(materializerSettings)(system)

    implicit val executionContext = system.dispatchers.lookup("xyz-dispatcher")

    val esClient = ElasticsearchClient()

    val httpConfig = config.getConfig("http")

    val route: Route = pathSingleSlash {
      complete("后端程序还活着！")
    } ~ OfflineQueryRoutes() ~ StreamControlRoutes(esClient)

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
