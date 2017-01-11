package com.hoolix.processor.http.routes

import java.time.Instant

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import com.hoolix.processor.streams.KafkaToEsStream
import com.typesafe.config.Config
import org.elasticsearch.client.transport.TransportClient

import scala.concurrent.ExecutionContext

/**
  * Hoolix 2017
  * Created by simon on 1/10/17.
  */
object StreamControlRoutes {
  def apply(esClient: TransportClient)(implicit config: Config, system: ActorSystem, ec: ExecutionContext, mat: Materializer): Route = {
    pathPrefix("start"/ IntNumber / Remaining) { (parallelism, kafkaTopic) =>
      val stream = KafkaToEsStream(
        parallelism,
        esClient,
        kafkaTopic
      )
      stream.run()
      complete(s"pipeline $kafkaTopic started")
    } ~
    pathPrefix("stop" / Remaining) { kafkaTopic =>
      println(s"Stopping Kafka Source now... - " + Instant.now)
      KafkaToEsStream.stopStream(kafkaTopic)
      complete(s"pipeline $kafkaTopic stopped")
    } ~
    pathPrefix("shutdown" / Remaining) { kafkaTopic =>
      println(s"Shutting down Kafka Source now... - " + Instant.now)
      KafkaToEsStream.shutdownStream(kafkaTopic)
      complete(s"pipeline $kafkaTopic shutdown")
    }
  }
}
