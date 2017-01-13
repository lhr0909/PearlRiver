package com.hoolix.processor.flows

import java.io.File

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.hoolix.processor.filters.Filter
import com.hoolix.processor.filters.Filter.ConditionedFilter
import com.hoolix.processor.filters.loaders.ConfigLoader
import com.hoolix.processor.models.{Event, KafkaTransmitted}

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
  val filtersMap = Map(
    "*" -> directory.list().map((filename) => {
      println(filename)
      filename.split("\\.")(0) -> ConfigLoader.build_from_local("conf/pipelinejson/" + filename)("*")("*")
    }).toMap
  )

  println(filtersMap)

  // 默认从本地和数据库中加载配置，，本地是对所有用户的默认的配置，，数据库中是针对特定用户的


  def apply(parallelism: Int): Flow[KafkaTransmitted, (KafkaTransmitted, Seq[ConditionedFilter]), NotUsed] = {

    Flow[KafkaTransmitted].mapAsync(parallelism) { kafkaTransmitted: KafkaTransmitted =>
          val payload = kafkaTransmitted.event.toPayload
          val token = payload.get("token") match {
            case Some(t) => t.asInstanceOf[String]
            case None => "*"
          }
          val `type` = payload.get("type") match {
            case Some(t) => t.asInstanceOf[String]
            case None => "*"
          }
          val tokenFilters: Map[String, Seq[ConditionedFilter]] = filtersMap.get(token) match {
            case Some(map: Map[String, Seq[ConditionedFilter]]) => map
            case None => filtersMap("*")
          }
          val filtersSeq = tokenFilters.get(`type`) match {
            case Some(seq) => seq
            case None => Seq()
          }
          // 如果没有找到，再去加载一次数据库，应该要更新filtersMap中的内容
          Future.successful((kafkaTransmitted, filtersSeq))

    }
  }
}
