package com.hoolix.processor.http.routes

import akka.actor.ActorSystem
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import com.hoolix.processor.filters.loaders.ConfigLoader
import com.hoolix.processor.models.events.Event
import com.hoolix.processor.streams.PreviewStream
import com.typesafe.config.Config
import org.json4s.jackson.JsonMethods.parse
import spray.json.JsValue
import org.json4s._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/**
  * Created by peiyuchao on 2017/1/18.
  */
object PreviewRoutes {
  def apply()(implicit config: Config, system: ActorSystem, ec: ExecutionContext, mat: Materializer): Route = {
    implicit val formats = DefaultFormats
    path("preview") {
      post {
        entity(as[JsValue]) { jsonObj =>
          val json = jsonObj.toString
          println(json)
          //          println("will not print this")
          val sample = (parse(json) \ "records").extract[Seq[String]]
          val `type` = (parse(json) \ "message_type").extract[String]
          val token = "*" // TODO parse header http://doc.akka.io/docs/akka-http/current/scala/http/common/http-model.html#header-model
        //          val configsString: String = jsonObj.asJsObject.fields("configs").convertTo[String]
        val configsString: String = jsonObj.asJsObject.fields("configs").toString()
          //          println(configsString)
          val configs = ConfigLoader.parse_from_json(configsString)
          //          val configs = ConfigLoader.parse_from_json((parse(json) \ "configs").extract[String])
          val filters = ConfigLoader.build_filter(configs)("*")("*")
          val parallelism = 20


          val stream = new PreviewStream(sample, token, `type`, filters, parallelism, config, system, ec)
          var events = List[Event]()
          val future = stream.stream.runForeach((shipper) => events = events.::(shipper.event))
          // TODO 整理输出的格式
          //          onSuccess(future) {
          //            complete(events.toString())
          //          }
          //          future.onComplete({
          //            case Success(result) => {
          //              println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + events)
          //            }
          //            case Failure(e) => {
          //              e.printStackTrace()
          //            }
          //
          //          })

          onComplete(future) {
            case Success(result) => {
              println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + events)
              complete(events.toString())
            }
            case Failure(e) => {
              e.printStackTrace()
              complete(e)
            }
          }
          ////          stream.run()
          ////          stream.
          //          complete(events.toString())
        }
      }
    }
  }
}
