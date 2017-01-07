package com.hoolix.processor.flows

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.hoolix.processor.models.KafkaTransmitted
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.TimeValue

import scala.concurrent.{ExecutionContext, Future, Promise}

/**
  * Hoolix 2017
  * Created by simon on 1/7/17.
  */
object CreateIndexFlow {

  lazy val defaultMapping: String = scala.io.Source.fromFile("conf/es-default-mapping.json").mkString

  def defaultTimeout = new TimeValue(500, TimeUnit.MILLISECONDS)

  def apply(
           parallelism: Int,
           esClient: TransportClient
           )(implicit ec: ExecutionContext): Flow[KafkaTransmitted, KafkaTransmitted, NotUsed] = {
    Flow[KafkaTransmitted].mapAsync[KafkaTransmitted](parallelism) { event =>
      val p = Promise[KafkaTransmitted]()

      Future {
        val existsResponse = esClient.admin().indices()
          .prepareExists(event.indexName).get(defaultTimeout)
        if (!existsResponse.isExists) {
          val createResponse = esClient.admin().indices()
            .prepareCreate(event.indexName)
            .setSettings(
              Settings.builder()
                .put("index.refresh_interval", "5s")
                .put("index.number_of_shards", "3")
                .put("index.number_of_replicas", "1")
//                .put("index.routing.allocation.require.box_type", "hot")
                .put("index.requests.cache.enable", "true")
            )
            .addMapping("_default_", defaultMapping)
            .get(defaultTimeout)
          p.success(event)
        } else {
          p.success(event)
        }
      }

      p.future
    }
  }
}
