package com.hoolix.processor.filters

import com.hoolix.processor.models.{Event, IntermediateEvent}
import scala.util.Random

/**
  * Created by peiyuchao on 16/8/16.
  */

case class RegexBasedAnomalyDetectionFilter(params: Seq[Seq[String]]) extends Filter {
  override def handle(event: Event): Event = {
    val payload = event.toPayload
    params.foreach((param) => {
      val Seq(field, value, anomaly_field, anomaly_value) = param
      if (payload.get(field).asInstanceOf[String] matches value) {
        payload.put(anomaly_field, anomaly_value)
      }
    })
    IntermediateEvent(payload)
  }
}

case class RandomAnomalyDetectionFilter(percentage: Double, distribution: Seq[Seq[String]], anomalies: Seq[String]) extends Filter {
  var bounds: Seq[(String, Int, Int)] = {
    var tempBounds: Seq[(String, Int, Int)] = Seq()
    for (i <- distribution.indices) {
      var lowerBound: Int = 0
      var upperBound: Int = 0
      for (j <- 0 until i) {
        lowerBound += distribution(j)(1).toInt
        upperBound += distribution(j)(1).toInt
      }
      upperBound += distribution(i)(1).toInt
      tempBounds = tempBounds.:+((distribution(i)(0), lowerBound, upperBound))
    }
    tempBounds
  }

  override def handle(event: Event): Event = {
    val payload = event.toPayload
    anomalies.foreach((anomaly) => {
      if (new Random().nextDouble() < percentage / 100) {
        var level: String = null
        val rand = new Random().nextInt(100)
        for (i <- bounds.indices) {
          if (rand >= bounds(i)._2 && rand < bounds(i)._3) {
            level = bounds(i)._1
            // TODO break
          }
        }
        if (level != null)
          payload.put(anomaly, level)
      }
    })
    IntermediateEvent(payload)
  }
}
