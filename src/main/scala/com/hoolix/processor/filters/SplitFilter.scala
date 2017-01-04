package com.hoolix.processor.filter

import com.hoolix.pipeline.core.{Context, Filter, FilterConfig, PreviewContext}
import com.hoolix.pipeline.util.Utils
import com.hoolix.processor.filters.Filter
import com.hoolix.processor.models.Event

import scala.annotation.tailrec
import scala.collection.mutable

case class SplitFilter(targetField: String, delimiter: String, limit: Int = -1, names: Seq[String] = Seq()) extends Filter {
  override def handle(event: Event): Event = {
    if (targetField != null && targetField != "") {
      val payload = event.toPayload
      if (payload.containsKey(targetField)) {
        val targetValue = payload.get(targetField).asInstanceOf[String]
        if (targetValue != "") {
          val values = targetValue.split(delimiter, limit)
          names.size match {
            case 0 => values.zipWithIndex.foreach()
            case _ => values.zip(names).foreach() // TODO 如果 names 与 values 的长度不同
          }

        }

      }


    }
    val field = ctx.get(cfg.target, "")

    if (field == "")
      return Left(null)

    val words = field.split()

    val result = if (names.size == 0)
      words.zipWithIndex.map{ case (value, index) => (String.valueOf(index),value)}.toMap
    else
      names.zip(words)

    Right(result)
  }

  override def handle_preview(ctx: PreviewContext): Either[Throwable, Iterable[(String,(Int,Int, Any))]] = {
    val (start, end, value) = ctx.get_preview(cfg.target, (-1,-1,""))
    if (value == "")
      return Left(null)

    val buffer = Utils.split_with_position(value, delimiter, limit)

    val result = names.size match {
      case 0 => buffer.zipWithIndex.map { case (value, index) => (String.valueOf(index),value)}
      case _ => names.zip(buffer)
    }

    return Right(result)
  }
}
