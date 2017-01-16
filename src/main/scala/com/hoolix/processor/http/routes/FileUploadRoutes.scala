package com.hoolix.processor.http.routes

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.hoolix.processor.decoders.RawLineDecoder
import com.hoolix.processor.flows.DecodeFlow
import com.hoolix.processor.models.{ElasticsearchPortFactory, FileSourceMetadata}
import com.hoolix.processor.sources.ByteStringToEsSource

/**
  * Hoolix 2017
  * Created by simon on 1/13/17.
  */
object FileUploadRoutes {

  def apply(): Route = {
    pathPrefix("file" / JavaUUID / Segment / Remaining) { (indexAlias, logType, tag) =>
      post {
        extractRequestContext { ctx =>
          fileUpload("file") {
            case (metadata, byteSource) =>
              ByteStringToEsSource(parallel = 1, metadata.fileName, byteSource).source()
                .via(DecodeFlow[FileSourceMetadata, ElasticsearchPortFactory](
                  parallelism = 1,
                  RawLineDecoder(indexAlias.toString, logType, Seq(tag), "file")
                ).flow)
              complete(s"$indexAlias, $logType, $tag")
          }
        }
      }
    }
  }
}
