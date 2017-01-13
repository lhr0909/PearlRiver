package com.hoolix.processor.filters

import com.hoolix.processor.models.{Event, IntermediateEvent, IntermediatePreviewEvent}
import com.hoolix.processor.utils.Utils

case class SplitFilter(targetField: String, delimiter: String, limit: Int = -1, names: Seq[String] = Seq()) extends Filter {
  override def handle(event: Event): Event = {
    // TODO 如果解析失败
    val payload = event.toPayload
    if (targetField != null && targetField != "") {
      if (payload.contains(targetField)) {
        val targetValue = payload.get(targetField).asInstanceOf[Some[String]].get
        if (targetValue != "") {
          val values = targetValue.split(delimiter, limit)
          val pairs = names.size match {
            case 0 => values.zipWithIndex.map{ case (value, index) => (String.valueOf(index), value)}.toMap
            case _ => names.zip(values) // TODO 如果 names 与 values 的长度不同
          }
          pairs.foreach({
            case (name, value) => payload.put(name, value)
          })

        }
      }
    }

    IntermediateEvent(payload)
  }

  override def handle_preview(event: Event): Event = {

    val payload = event.asInstanceOf[IntermediatePreviewEvent].toPayload
    val highlights = event.asInstanceOf[IntermediatePreviewEvent].highlights
    val (start: Int, end: Int) = highlights(targetField).asInstanceOf[(Int, Int)]
    val value: String = payload(targetField).asInstanceOf[String]


//    val (start, end, value) = ctx.get_preview(cfg.target, (-1,-1,""))
    if (value == "") {
      None
    } else {
      val buffer: Iterable[(Int,Int,String)] = Utils.split_with_position(value, delimiter, limit)

      val result: Iterable[(String,(Int,Int, Any))] = names.size match {
        case 0 => buffer.zipWithIndex.map { case (value, index) => (String.valueOf(index),value)}
        case _ => names.zip(buffer)
      }

      result.foreach((tuple) => {
        highlights.put(tuple._1, (tuple._2._1, tuple._2._2))
        payload.put(tuple._1, tuple._2._3)
      })

    }
    IntermediatePreviewEvent(highlights, payload)
  }
}
