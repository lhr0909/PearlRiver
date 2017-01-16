package com.hoolix.processor.http.routes

import akka.actor.ActorSystem
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import com.hoolix.processor.streams.FileToEsStream
import com.typesafe.config.Config
import org.elasticsearch.client.transport.TransportClient

import scala.concurrent.ExecutionContext

/**
  * Hoolix 2017
  * Created by simon on 1/13/17.
  */
object FileUploadRoutes {

  def apply(esClient: TransportClient)(implicit config: Config, system: ActorSystem, ec: ExecutionContext): Route = {
    pathPrefix("file" / JavaUUID / Segment / Remaining) { (indexAlias, logType, tag) =>
      post {
        extractRequestContext { ctx =>
          implicit val materializer = ctx.materializer

          fileUpload("file") {

            case (fileInfo, byteStringSource) =>
              FileToEsStream(
                parallelism = 1,
                esClient,
                fileInfo,
                byteStringSource,
                indexAlias.toString,
                logType,
                tag
              ).stream.run()

              complete(s"$indexAlias, $logType, $tag")
          }
        }
      }
    }
  }
}
