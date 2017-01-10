package com.hoolix.processor.flows

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.hoolix.processor.models.KafkaTransmitted
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.TimeValue
import scala.util.{Failure, Success}
import scala.io.Source
import scala.concurrent.duration._
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Hoolix 2017
  * Created by simon on 1/7/17.
  */
object CreateIndexFlow {

  lazy val defaultMapping: String = Source.fromFile("conf/es-default-mapping.json").mkString

  def defaultTimeout = new TimeValue(500, TimeUnit.MILLISECONDS)

  def apply(
           parallelism: Int,
           esClient: TransportClient
           )(implicit ec: ExecutionContext): Flow[KafkaTransmitted, KafkaTransmitted, NotUsed] = {
    Flow[KafkaTransmitted].mapAsync[KafkaTransmitted](parallelism) { event =>
      println("=============================================================")
      println(event)
      println(event.indexType)

      val p = Promise[KafkaTransmitted]()


      def exist(): Future[IndicesExistsResponse] = {
        val promise = Promise[IndicesExistsResponse]()
        val listener = new ActionListener[IndicesExistsResponse] {
          override def onResponse(response: IndicesExistsResponse) = promise.success(response)

          override def onFailure(e: Exception) = promise.failure(e)
        }
        esClient.admin().indices().prepareExists(event.indexName).execute(listener)
        promise.future
      }

      def create(): Future[CreateIndexResponse] = {
        val settings: Settings.Builder = Settings.builder()
          .put("index.refresh_interval", "5s")
          .put("index.number_of_shards", "3")
          .put("index.number_of_replicas", "1")
          .put("index.requests.cache.enable", "true")
        val mapping = Source.fromFile("conf/mapping/" + event.indexType + ".mapping.json").mkString
        val promise = Promise[CreateIndexResponse]()
        val listener = new ActionListener[CreateIndexResponse] {
          override def onResponse(response: CreateIndexResponse) = promise.success(response)
          override def onFailure(e: Exception) = promise.failure(e)
        }
        esClient.admin().indices().prepareCreate(event.indexName)
          .setSettings(settings)
          .addMapping(event.indexType, mapping)
          .execute(listener)
        promise.future
      }


      Future {
        val futureExist = exist()
        futureExist.onComplete({
          case Success(valueExist) => {
            if (valueExist.isExists) {
              p.success(event)
            } else {
              val futureCreate = create()
              futureCreate.onComplete({
                case Success(valueCreate) => {
                  p.success(event)
                }
                case Failure(e) => e.printStackTrace()
              })
            }
          }
          case Failure(e) => e.printStackTrace()
        })
      }

      p.future







//      val p = Promise[KafkaTransmitted]()
//      Future {
//        val existsResponse = esClient.admin().indices()
//          .prepareExists(event.indexName).get(defaultTimeout)
//        if (!existsResponse.isExists) {
//          p.success(event)
//        } else {
//          p.success(event)
//        }
//      }

//      Future {
////        blocking {
//          val existsResponse = esClient.admin().indices()
//            .prepareExists(event.indexName).get(defaultTimeout)
//          println("check exist is " + existsResponse.isExists)
//          if (!existsResponse.isExists) {
//            println(event.indexName)
//            println(event.indexType)
//            val createResponse = esClient.admin().indices()
//              .prepareCreate(event.indexName)
//              .setSettings(settings)
////                          .addMapping("_default_", defaultMapping)
//              .addMapping(event.indexType, mapping)
//              .get(defaultTimeout)
//            println(">>>>>>")
//            println(createResponse)
//            p.success(event)
//          } else {
//            p.success(event)
//          }
////        }
//      }
//      println("finish create?")
//
//      p.future

    }
  }
}
