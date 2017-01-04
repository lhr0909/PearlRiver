package com.hoolix.pipeline.filter

import com.hoolix.pipeline.core.{Context, Filter, FilterConfig, PreviewContext}
import com.hoolix.pipeline.util.Utils

case class KVFilter(cfg:FilterConfig, delimiter:String="\\s+", sub_delimiter:String="=") extends Filter {

  override def handle(ctx: Context): Either[Throwable, Iterable[(String,Any)]] = {

    if (sub_delimiter == "") {
      // do not need to split twice
      Left(null)
    } else {
      val field_value = ctx.get(cfg.target, "")
      if (field_value == null || field_value == "")
        return Left(null)

      val words = field_value.split(delimiter)
      Right(words.flatMap { word => {
        val kvs = word.split(sub_delimiter, 2)
        kvs.size match {
          case 0 => None
          case 1 => None
          case 2 => Some((kvs(0), kvs(1)))
        }
      }})
    }
  }

  override def handle_preview(ctx: PreviewContext): Either[Throwable, Iterable[(String,(Int,Int, Any))]] = {
    val (start, end, field_value) = ctx.get_preview(cfg.target, (-1, -1, ""))
    if (field_value == null || field_value == "")
      return Left(null)

    Right(
      Utils.split_with_position(field_value, delimiter).flatMap { case (start, _, kv) => {
        if (kv == "") {
          None
        }
        else {
          val triple = Utils.split_with_position(kv, sub_delimiter, 2).toSeq
          if (triple.size == 1) {
            Some((triple(0)._3, (start + triple(0)._1, start + triple(0)._2, triple(0)._3)))
          }
          else {
            val (_, _, name) = triple(0)
            val (value_start, value_end, value) = triple(1)
            Some((name, (start + value_start, start + value_end, value)))
          }
        }
      }}
    )
  }
}

