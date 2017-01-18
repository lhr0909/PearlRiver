package com.hoolix.processor.flows

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.hoolix.processor.filters.Filter.ConditionedFilter
import com.hoolix.processor.models.events.Event
import com.hoolix.processor.models.{PortFactory, Shipper, SourceMetadata}
import scala.concurrent.Future

/**
  * Created by peiyuchao on 2017/1/13.
  */
case class FilterPreviewFlow[SrcMeta <: SourceMetadata, PortFac <: PortFactory](parallelism: Int) {

  type Shipperz = Shipper[SrcMeta, PortFac]

  def flow(): Flow[(Shipperz, Seq[ConditionedFilter]), Shipperz, NotUsed] = {
    Flow[(Shipperz, Seq[ConditionedFilter])].mapAsync(parallelism)((pair) => {
      val (shipper, filters) = pair
      var filtered: Event = shipper.event
      filters.filter(_._1.forall(_ (filtered))).foreach((elem) => filtered = elem._2.handle_preview(filtered))
      Future.successful(Shipper(
        filtered,
        shipper.sourceMetadata,
        shipper.portFactory
      ))
    }).named("filter-preview-flow")
  }
}
