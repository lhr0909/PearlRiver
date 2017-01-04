//package com.hoolix.pipeline.decoder
//
//import com.hoolix.pipeline.core.{Context, Decoder, MetricTypes}
//import com.hoolix.pipeline.filter.DateFilter
//import org.joda.time.DateTime
//import org.slf4j.LoggerFactory
//
//case class RawLineDecoder(token:String="_", typ:String="*", tag:String="") extends Decoder{
//  lazy val logger = LoggerFactory.getLogger(this.getClass)
//  override def decode(ctx: Context): Boolean = {
//
//    ctx.put("token", token)
//    ctx.put("type" , typ)
//    ctx.put("tag"  , tag)
//    ctx.put("message", ctx.record)
//
//    ctx.message = ctx.record
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
