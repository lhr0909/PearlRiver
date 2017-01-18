package com.hoolix.processor.http.routes

import java.time.Instant

import akka.actor.ActorSystem
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import com.hoolix.processor.filters.loaders.ConfigLoader
import com.hoolix.processor.streams.KafkaToEsStream
import com.typesafe.config.Config
import org.elasticsearch.client.transport.TransportClient
import org.json4s._
import org.json4s.jackson.JsonMethods._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import com.hoolix.processor.models.events.Event

import scala.concurrent.ExecutionContext
import com.hoolix.processor.streams.PreviewStream


import scala.util.{Failure, Success}

/**
  * Hoolix 2017
  * Created by simon on 1/10/17.
  */
object StreamControlRoutes {
  def apply(esClient: TransportClient)(implicit config: Config, system: ActorSystem, ec: ExecutionContext, mat: Materializer): Route = {
    implicit val formats = org.json4s.DefaultFormats
    pathPrefix("start"/ IntNumber / Remaining) { (parallelism, kafkaTopic) =>
      val stream = KafkaToEsStream(
        parallelism,
        esClient,
        kafkaTopic,
        { _ =>
          println("done, shutting down now")
          KafkaToEsStream.shutdownStream(kafkaTopic)
        }
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
      complete(s"pipeline $kafkaTopic stopped")
    }
  }
}
