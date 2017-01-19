package com.hoolix.processor.http.routes

import akka.actor.ActorSystem
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import com.hoolix.processor.filters.loaders.ConfigLoader
import com.hoolix.processor.models.events.IntermediatePreviewEvent
import com.hoolix.processor.streams.PreviewStream
import com.typesafe.config.Config
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import spray.json.{JsValue, JsonPrinter}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport.sprayJsonMarshallerConverter
import com.fasterxml.jackson.annotation.JsonFormat

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/**
  * Created by peiyuchao on 2017/1/18.
  */
object PreviewRoutes {
  def apply()(implicit config: Config, system: ActorSystem, ec: ExecutionContext, mat: Materializer): Route = {
    implicit val formats = DefaultFormats
//    implicit val writer = JsonPrinter
//    implicit val sprayFormat = sprayJsonMarshaller[List[Map[String, (Any, Int, Int)]]]
    path("preview") {
      post {
        entity(as[JsValue]) { jsonObj =>

          val json = jsonObj.toString
          println(json)
          //          println("will not print this")
          val sample = (parse(json) \ "records").extract[Seq[String]]
          val `type` = (parse(json) \ "message_type").extract[String]
          // TODO parse header http://doc.akka.io/docs/akka-http/current/scala/http/common/http-model.html#header-model
          val token = "*" // headerValueByName("token")
        //          val configsString: String = jsonObj.asJsObject.fields("configs").convertTo[String]
        val configsString: String = jsonObj.asJsObject.fields("configs").toString()
          //          println(configsString)
          val configs = ConfigLoader.parse_from_json(configsString)
          //          val configs = ConfigLoader.parse_from_json((parse(json) \ "configs").extract[String])
          val filters = ConfigLoader.build_filter(configs)("*")("*")
          val parallelism = 20


          val stream = new PreviewStream(sample, token, `type`, filters, parallelism, config, system, ec)
//          var events = List[Event]()
          var events = List[Map[String, (Any, Int, Int)]]()
          val future = stream.stream.runForeach((shipper) => {
            val event = shipper.event.asInstanceOf[IntermediatePreviewEvent]
            events = events.::(event.toPayload.filter((pair) => {
              event.highlights(pair._1) != (-1, -1)
            }).map((pair) => {
              val highlight = event.highlights(pair._1).asInstanceOf[(Int, Int)]
              pair._1 -> (pair._2, highlight._1, highlight._2)
            }).toMap)
          })

          onComplete(future) {
            case Success(result) => {
              println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + events)
              complete(events.toString()) // TODO to json
            }
            case Failure(e) => {
              e.printStackTrace()
              complete(e)
            }
          }

        }
      }
    }
  }
}
