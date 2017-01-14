package com.hoolix.processor.sources

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import com.hoolix.processor.models._
import com.typesafe.config.Config

import scala.concurrent.Future

/**
  * Hoolix 2017
  * Created by simon on 1/1/17.
  */
abstract class AbstractSource {
  type SrcMeta <: SourceMetadata
  type PortFac <: PortFactory

  type S
  type Mat

  val parallelism: Int

  val startingSource: Source[S, Mat]

  val sourceType: String = "abstract"

  def convertToShipper(incoming: S): Future[Shipper[SrcMeta, PortFac]]

  def source()(implicit config: Config, system: ActorSystem): Source[Shipper[SrcMeta, PortFac], Mat] = {
    startingSource.mapAsync(parallelism)(convertToShipper).named(s"$sourceType-source")
  }
}

