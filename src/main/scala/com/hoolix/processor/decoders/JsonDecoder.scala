package com.hoolix.pipeline.decoder

import com.hoolix.pipeline.filter.DateFilter
import com.hoolix.pipeline.core.{Context, Decoder, MetricTypes}
import org.joda.time.DateTime
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import org.slf4j.LoggerFactory

object JsonDecoder {
  val meta_type = "type"
  val meta_tag  = "tag"
  val meta_token= "token"
  val meta_host= "host"
  val meta_message= "message"
  val meta_timestamp= "@timestamp"
  val meta_version= "@version"
}

case class JsonDecoder() extends Decoder {
  lazy val logger = LoggerFactory.getLogger(this.getClass)
  implicit val formats = DefaultFormats
  override def decode(ctx: Context): Boolean = {

    try {
      //TODO message length check
      //TODO parse as mutable map
      val jnode = parse(ctx.record)
      //ctx.result = jnode.extractOpt[scala.collection.mutable.HashMap[String,String]].getOrElse(scala.collection.mutable.HashMap())
      for ((k,v) <- jnode.extractOpt[Map[String,String]].getOrElse(Map())) {
        ctx.put(k,v)
      }
    } catch {
      case e: Throwable =>
        ctx.metric(MetricTypes.metric_decode_fail)
        //ctx.metric.value.count(env, "total", XYZMetric.Type.JsonFail)
        //env.logger.error("parse json failed: " + input)
        logger.error("error: parse json failed " + e.getMessage + " " + ctx.record)
        return false;
    }

    if (!ctx.has("@timestamp"))
      ctx.put("@timestamp", DateFilter.now())

    ctx.message = ctx.get("message", "")

    ctx.token = ctx.get("token", "")
    ctx.typ   = ctx.get("type", "")

    ctx.upload_timestamp = try {
      DateFilter.dest_format.parseDateTime("@timestamp")
    }catch {
      case e: Throwable => new DateTime()
    }

    true
  }
}
