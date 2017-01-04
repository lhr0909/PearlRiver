package com.hoolix.pipeline.filter

import com.hoolix.pipeline.core.{Context, Filter, FilterConfig}
import scala.util.Random

/**
  * Created by peiyuchao on 16/8/16.
  */

case class RegexBasedAnomalyDetectionFilter(cfg: FilterConfig, params: Seq[Seq[String]]) extends Filter {
  override def handle(ctx: Context): Either[Throwable, Iterable[(String,Any)]] = {
    Right(
      params.flatMap { param => {
        val Seq(field, value, anomaly_field, anomaly_value) = param
        if (ctx.get((cfg.pool, field), "") matches value) {
          Some((anomaly_field, anomaly_value))
        }
        else {
          None
        }
      }}
    )
  }
}

case class RandomAnomalyDetectionFilter(cfg: FilterConfig, percentage: Double, distribution: Seq[Seq[String]], anomalies: Seq[String]) extends Filter {
  var bounds: Seq[Tuple3[String, Int, Int]] = {
    var tempBounds: Seq[Tuple3[String, Int, Int]] = Seq()
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

  override def handle(ctx: Context): Either[Throwable, Iterable[(String, Any)]] = {
    Right(
      anomalies.flatMap { anomaly => {
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
            Some((anomaly, level))
          else
            None
        }
        else
          None
      }
      }
    )
  }
}
