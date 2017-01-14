package com.hoolix.processor.flows

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.hoolix.processor.models._
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
}

case class CreateIndexFlow(
                            parallelism: Int,
                            esClient: TransportClient,
                            indexSettings: Settings.Builder
                          ) {
//  lazy val defaultMapping: String = scala.io.Source.fromFile("conf/es-default-mapping.json").mkString

  type SrcMeta <: SourceMetadata
  type PortFac <: PortFactory

  type Shipperz = Shipper[SrcMeta, PortFac]

  def getIndexName(shipper: Shipperz): String = {
    shipper.portFactory match {
      case epf: ElasticsearchPortFactory => epf.generateIndexName(shipper.event)
      case _ => "_unknown_"
    }
  }

  def getIndexType(shipper: Shipperz): String = {
    shipper.portFactory match {
      case epf: ElasticsearchPortFactory => epf.generateIndexType(shipper.event)
      case _ => "_unknown_"
    }
  }

  def exist(
             esClient: TransportClient,
             event: Shipperz
           )(implicit ec: ExecutionContext): Future[IndicesExistsResponse] = {

    if (CreateIndexFlow.createdIndexCache.contains(getIndexName(event))) {
      Future.successful(new IndicesExistsResponse(true))
    } else {
      val promise = Promise[IndicesExistsResponse]()
      val listener = new ActionListener[IndicesExistsResponse] {
        override def onResponse(response: IndicesExistsResponse) = promise.success(response)

        override def onFailure(e: Exception) = promise.failure(e)
      }
      esClient.admin().indices().prepareExists(getIndexName(event)).execute(listener)

      promise.future
    }
  }

  def create(
              esClient: TransportClient,
              indexSettings: Settings.Builder,
              event: Shipperz
            )(implicit ec: ExecutionContext): Future[CreateIndexResponse] = {

    val mapping = Source.fromFile("conf/mapping/" + getIndexType(event) + ".mapping.json").mkString
    val promise = Promise[CreateIndexResponse]()
    val listener = new ActionListener[CreateIndexResponse] {
      override def onResponse(response: CreateIndexResponse) = promise.success(response)
      override def onFailure(e: Exception) = promise.failure(e)
    }
    esClient.admin().indices().prepareCreate(getIndexName(event))
      .setSettings(indexSettings)
      .addMapping(getIndexType(event), mapping)
      .execute(listener)

    promise.future
  }

  def flow(implicit ec: ExecutionContext): Flow[Shipperz, Shipperz, NotUsed] = {
    Flow[Shipperz].mapAsync[Shipperz](parallelism) { event =>

      val p = Promise[Shipperz]()

      Future {
        exist(esClient, event) onComplete {
          case Success(valueExist) =>
            if (valueExist.isExists) {
              CreateIndexFlow.createdIndexCache.put(getIndexName(event), true)
              p.success(event)
            } else {
              println(s"index ${ getIndexName(event) } does not exist, creating it")
              create(esClient, indexSettings, event) onComplete {
                case Success(_) =>
                  println(s"successfully created index ${ getIndexName(event) }")
                  CreateIndexFlow.createdIndexCache.put(getIndexName(event), true)
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
