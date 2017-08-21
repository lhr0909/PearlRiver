package io.divby0.pearlriver.http.routes

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route

/**
  * Hoolix 2017
  * Created by simon on 1/5/17.
  */
object OfflineQueryRoutes {
  def apply(): Route = {
    pathPrefix("offlinequery" / "register") {
      post {
        println("POST offlinequery/register")
        complete("registered")
      }
    } ~
    pathPrefix("offlinequery" / "retrieve" / Remaining) { id =>
      get {
        complete(s"retrieve $id")
      }
    } ~
    pathPrefix("offlinequery" / "retry" / Remaining) { id =>
      get {
        complete(s"retry $id")
      }
    } ~
    pathPrefix("offlinequery" / "delete" / Remaining) { id =>
      delete {
        complete(s"delete $id")
      }
    }
  }
}
