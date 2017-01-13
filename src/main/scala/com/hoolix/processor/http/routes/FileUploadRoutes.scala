package com.hoolix.processor.http.routes

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.hoolix.processor.sources.ByteStringSource

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
              ByteStringSource(parallelism = 1, metadata.fileName, byteSource)
              complete(s"$indexAlias, $logType, $tag")
          }
        }
      }
    }
  }
}