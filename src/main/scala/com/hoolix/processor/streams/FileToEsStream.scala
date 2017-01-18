package com.hoolix.processor.streams

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.server.directives.FileInfo
import akka.stream.scaladsl.{Keep, RunnableGraph, Source}
import akka.util.ByteString
import com.hoolix.processor.decoders.RawLineDecoder
import com.hoolix.processor.filters.loaders.ConfigLoader
import com.hoolix.processor.flows.{ElasticsearchCreateIndexFlow, DecodeFlow, ElasticsearchBulkRequestFlow, FilterFlow}
import com.hoolix.processor.models.{ElasticsearchPortFactory, FileSourceMetadata, Shipper}
import com.hoolix.processor.modules.ElasticsearchClient
import com.hoolix.processor.sources.ByteStringToEsSource
import com.typesafe.config.Config
import org.elasticsearch.client.transport.TransportClient

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
  * Hoolix 2017
  * Created by simon on 1/16/17.
  */
object FileToEsStream {

  def apply(
             parallelism: Int,
             esClient: TransportClient,
             fileInfo: FileInfo,
             byteStringSource: Source[ByteString, Any],
             indexAlias: String,
             logType: String,
             tag: String,
             onComplete: (Try[Done]) => Unit
           )(implicit config: Config, system: ActorSystem, ec: ExecutionContext): FileToEsStream =
    new FileToEsStream(parallelism, esClient, fileInfo, byteStringSource, indexAlias, logType, tag, onComplete, config, system, ec)

  class FileToEsStream(
                         parallelism: Int,
                         esClient: TransportClient,
                         fileInfo: FileInfo,
                         byteStringSource: Source[ByteString, Any],
                         indexAlias: String,
                         logType: String,
                         tag: String,
                         onComplete: (Try[Done]) => Unit,
                         implicit val config: Config,
                         implicit val system: ActorSystem,
                         implicit val ec: ExecutionContext
                       ) {

    val futureExecutionContext: ExecutionContext = system.dispatchers.lookup("future-dispatcher")

    val fileSource = ByteStringToEsSource(parallelism, fileInfo.fileName, byteStringSource).source()

    val esBulkRequestSink = ElasticsearchBulkRequestFlow[FileSourceMetadata](esClient, parallelism)(config, futureExecutionContext)
    val esSink = esBulkRequestSink

    def stream: RunnableGraph[Future[Done]] = {

      val decodeFlow = DecodeFlow[FileSourceMetadata, ElasticsearchPortFactory](parallelism, RawLineDecoder(indexAlias, logType, Seq(tag), "file")).flow
      val apache_access = ConfigLoader.build_from_local("conf/pipeline/apache_access.yml")
      val apache_error = ConfigLoader.build_from_local("conf/pipeline/apache_error.yml")
      val nginx_access = ConfigLoader.build_from_local("conf/pipeline/nginx_access.yml")
      val nginx_error = ConfigLoader.build_from_local("conf/pipeline/nginx_error.yml")
      val mysql_error = ConfigLoader.build_from_local("conf/pipeline/mysql_error.yml")
      println(apache_access)
      println(apache_error)
      println(nginx_access)
      println(nginx_error)
      println(mysql_error)
      val filtersMap = Map(
        "*" -> Map(
          "apache_access" -> apache_access("*")("*"),
          "apache_error" -> apache_error("*")("*"),
          "nginx_access" -> nginx_access("*")("*"),
          "nginx_error" -> nginx_error("*")("*"),
          "mysql_error" -> mysql_error("*")("*")
        )
      )

      println(filtersMap)

      val filterFlow = FilterFlow[FileSourceMetadata, ElasticsearchPortFactory](parallelism, filtersMap).flow()
      val createIndexFlow = ElasticsearchCreateIndexFlow[FileSourceMetadata](
        parallelism,
        esClient,
        ElasticsearchClient.esIndexCreationSettings()
      ).flow(futureExecutionContext)

      fileSource.named("file-to-es-source")
        .viaMat(decodeFlow)(Keep.left).named("file-to-es-decode-flow")
        .viaMat(filterFlow)(Keep.left).named("file-to-es-filter-flow")
        .viaMat(createIndexFlow)(Keep.left).named("file-to-es-index-flow")
        .toMat(esSink.sink(onComplete))(Keep.right).named("file-to-es-sink")
    }
  }
}
