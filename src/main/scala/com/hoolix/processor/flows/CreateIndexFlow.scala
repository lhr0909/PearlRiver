package com.hoolix.processor.flows

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.hoolix.processor.models.KafkaTransmitted
import org.elasticsearch.ResourceAlreadyExistsException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.transport.RemoteTransportException

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

    val mapping = Source.fromFile("conf/mapping/" + event.indexType + ".mapping.json").mkString
    val promise = Promise[CreateIndexResponse]()
    val listener = new ActionListener[CreateIndexResponse] {
      override def onResponse(response: CreateIndexResponse) = promise.success(response)
      override def onFailure(e: Exception) = promise.failure(e)
    }
    esClient.admin().indices().prepareCreate(event.indexName)
      .setSettings(indexSettings)
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
              println(s"index ${ event.indexName } does not exist, creating it")
              create(esClient, indexSettings, event) onComplete {
                case Success(_) =>
                  println(s"successfully created index ${ event.indexName }")
                  createdIndexCache.put(event.indexName, true)
                  p.success(event)

                case Failure(e: RemoteTransportException) =>
                  println("finding out root cause")
                  var e1: Throwable = e
                  while (e1.isInstanceOf[RemoteTransportException]) {
                    e1 = e1.getCause
                  }
                  e1 match {
                    case e: ResourceAlreadyExistsException =>
                      println(e.getMessage)
                      p.success(event)
                    case others => others.printStackTrace()
                  }

                case Failure(otherException) => otherException.printStackTrace()
              }
            }

          case Failure(existCallException) =>
            existCallException.printStackTrace()
        }
      }

      p.future
    }.named("create-index-flow")
  }
}
