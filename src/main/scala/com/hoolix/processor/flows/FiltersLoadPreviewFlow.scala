package com.hoolix.processor.flows

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.hoolix.processor.filters.Filter.ConditionedFilter
import com.hoolix.processor.models.{PortFactory, Shipper, SourceMetadata}
import scala.concurrent.Future

/**
  * Created by peiyuchao on 2017/1/13.
  */
case class FiltersLoadPreviewFlow[SrcMeta <: SourceMetadata, PortFac <: PortFactory](parallelism: Int, filters: Seq[ConditionedFilter]) {
  type Shipperz = Shipper[SrcMeta, PortFac]

  def flow: Flow[Shipperz, (Shipperz, Seq[ConditionedFilter]), NotUsed] = {
    Flow[Shipperz].mapAsync(parallelism) { shipper: Shipperz =>
      Future.successful((Shipper(
        shipper.event,
        shipper.sourceMetadata,
        shipper.portFactory
      ), filters))
    }.named("filters-load-preview-flow")
  }
}
