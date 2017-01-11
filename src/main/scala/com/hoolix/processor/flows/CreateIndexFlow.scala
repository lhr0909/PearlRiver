package com.hoolix.processor.flows

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.hoolix.processor.models.KafkaTransmitted
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.io.Source
import scala.util.{Failure, Success}

/**
  * Hoolix 2017
  * Created by simon on 1/7/17.
  */
object CreateIndexFlow {

  //TODO: get this index cache into SQL

  val createdIndexCache: TrieMap[String, Boolean] = TrieMap()

//  lazy val defaultMapping: String = scala.io.Source.fromFile("conf/es-default-mapping.json").mkString

  def exist(
             esClient: TransportClient,
             event: KafkaTransmitted
           )(implicit ec: ExecutionContext): Future[IndicesExistsResponse] = {

    if (createdIndexCache.contains(event.indexName)) {
      Future.successful(new IndicesExistsResponse(true))
    } else {
      val promise = Promise[IndicesExistsResponse]()
      val listener = new ActionListener[IndicesExistsResponse] {
        override def onResponse(response: IndicesExistsResponse) = promise.success(response)

        override def onFailure(e: Exception) = promise.failure(e)
      }
      esClient.admin().indices().prepareExists(event.indexName).execute(listener)

      promise.future
    }
  }

  def create(
              esClient: TransportClient,
              indexSettings: Settings.Builder,
              event: KafkaTransmitted
            )(implicit ec: ExecutionContext): Future[CreateIndexResponse] = {

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

  def apply(
             parallelism: Int,
             esClient: TransportClient,
             indexSettings: Settings.Builder
           )(implicit ec: ExecutionContext): Flow[KafkaTransmitted, KafkaTransmitted, NotUsed] = {
    Flow[KafkaTransmitted].mapAsync[KafkaTransmitted](parallelism) { event =>

      val p = Promise[KafkaTransmitted]()

      Future {
        exist(esClient, event) onComplete {
          case Success(valueExist) =>
            if (valueExist.isExists) {
              createdIndexCache.put(event.indexName, true)
              p.success(event)
            } else {
              create(esClient, indexSettings, event) onComplete {
                case Success(_) =>
                  createdIndexCache.put(event.indexName, true)
                  p.success(event)
                case Failure(e) => e.printStackTrace()
              }
            }
          case Failure(e) => e.printStackTrace()
        }
      }

      p.future
    }
  }
}
