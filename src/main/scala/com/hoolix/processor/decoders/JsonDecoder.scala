//package com.hoolix.processor.decoders
//
//
//import org.joda.time.DateTime
//import org.json4s.DefaultFormats
//import org.json4s.jackson.JsonMethods._
//import org.slf4j.LoggerFactory
//
//case class JsonDecoder() extends Decoder {
//  lazy val logger = LoggerFactory.getLogger(this.getClass)
//  implicit val formats = DefaultFormats
//  override def decode(ctx: Context): Boolean = {
//
//    try {
//      //TODO message length check
//      //TODO parse as mutable map
//      val jnode = parse(ctx.record)
//      //ctx.result = jnode.extractOpt[scala.collection.mutable.HashMap[String,String]].getOrElse(scala.collection.mutable.HashMap())
//      for ((k,v) <- jnode.extractOpt[Map[String,String]].getOrElse(Map())) {
//        ctx.put(k,v)
//      }
//    } catch {
//      case e: Throwable =>
//        ctx.metric(MetricTypes.metric_decode_fail)
//        //ctx.metric.value.count(env, "total", XYZMetric.Type.JsonFail)
//        //env.logger.error("parse json failed: " + input)
//        logger.error("error: parse json failed " + e.getMessage + " " + ctx.record)
//        return false;
//    }
//
//    if (!ctx.has("@timestamp"))
//      ctx.put("@timestamp", DateFilter.now())
//
//    ctx.message = ctx.get("message", "")
//
//    ctx.token = ctx.get("token", "")
//    ctx.typ   = ctx.get("type", "")
//
//    ctx.upload_timestamp = try {
//      DateFilter.dest_format.parseDateTime("@timestamp")
//    }catch {
//      case e: Throwable => new DateTime()
//    }
//
//    true
//  }
//}
