//package com.hoolix.pipeline.decoder
//
//import com.hoolix.pipeline.filter.DateFilter
//import com.hoolix.pipeline.core.{Context, Decoder, MetricTypes}
//import org.joda.time.DateTime
//import org.slf4j.LoggerFactory
//
///**
//  * token type tag streaming/file || message
//  */
//case class XYZLineDecoder() extends Decoder{
//  lazy val logger = LoggerFactory.getLogger(this.getClass)
//  override def decode(ctx: Context): Boolean = {
//
//    val words = ctx.record.split(" \\|\\| ", 2)
//    if (words.size != 2) {
//      logger.error("invalid input record :" +ctx.record)
//      ctx.metric(MetricTypes.metric_decode_fail)
//      return false
//    }
//    val head = words(0)
//    val body = words(1)
//
//    val headers = head.split(" ", 4)
//
//    if (headers.size != 4) {
//      logger.error("invalid input record head " +ctx.record)
//      ctx.metric(MetricTypes.metric_decode_fail)
//      return false
//    }
//
//    ctx.put("token", headers(0))
//    ctx.put("type" , headers(1))
//    ctx.put("tag"  , headers(2))
//    ctx.put("message", body)
//
//    ctx.message = body
//
//    //TODO streaming/upload
//
//    ctx.put("@timestamp", DateFilter.now())
//
//    ctx.token = ctx.get("token", "")
//    ctx.typ   = ctx.get("type", "")
//
//    ctx.upload_timestamp = new DateTime()
//
//    true
//  }
//}
