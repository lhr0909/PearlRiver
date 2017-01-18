package com.hoolix.processor.flows

import java.io.File

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.hoolix.processor.decoders.Decoder
import com.hoolix.processor.filters.Filter
import com.hoolix.processor.filters.Filter.ConditionedFilter
import com.hoolix.processor.filters.loaders.ConfigLoader
import com.hoolix.processor.models._

import scala.concurrent.Future

/**
  * Created by peiyuchao on 2017/1/11.
  */

object FiltersLoadFlow {
  // yaml
  //  val directory = new File("conf/pipeline")
  //  val filtersMap = Map(
  //    "*" -> directory.list().map((filename) => {
  //      println(filename)
  //      filename.split("\\.")(0) -> ConfigLoader.build_from_local("conf/pipeline/" + filename)("*")("*")
  //    }).toMap
  //  )

  // json
  val directory = new File("conf/pipelinejson")
  val defaultFilters = Map(
    "*" -> directory.list().map((filename) => {
      println(filename)
      filename.split("\\.")(0) -> ConfigLoader.build_from_local("conf/pipelinejson/" + filename)("*")("*")
    }).toMap
  )

  println(defaultFilters)
}

case class FiltersLoadFlow[SrcMeta <: SourceMetadata, PortFac <: PortFactory](parallelism: Int) {

  // TODO
  // 默认从本地和数据库中加载配置，，本地是对所有用户的默认的配置，，数据库中是针对特定用户的
  // 如果没有找到，再去加载一次数据库，应该要更新filtersMap中的内容

  type Shipperz = Shipper[SrcMeta, PortFac]

  def flow: Flow[Shipperz, (Shipperz, Seq[ConditionedFilter]), NotUsed] = {
    Flow[Shipperz].mapAsync(parallelism)((shipper: Shipperz) => {
      val payload = shipper.event.toPayload
      // TODO 没有找到应该报错
      val token = payload.get("token") match {
        case Some(t: String) => t
        case None => "*"
      }
      val `type` = payload.get("type") match {
        case Some(t: String) => t
        case None => "*"
      }
      val tokenFilters: Map[String, Seq[ConditionedFilter]] = FiltersLoadFlow.defaultFilters.get(token) match {
        case Some(map: Map[String, Seq[ConditionedFilter]]) => map
        case None => FiltersLoadFlow.defaultFilters("*")
      }
      val typeFilters = tokenFilters.get(`type`) match {
        case Some(seq: Seq[ConditionedFilter]) => seq
        case None => tokenFilters("*") //Seq()?
      }

      Future.successful((Shipper(
        shipper.event,
        shipper.sourceMetadata,
        shipper.portFactory
      ), typeFilters))
    }).named("filters-load-flow")
  }
}
