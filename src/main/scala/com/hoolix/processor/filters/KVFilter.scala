package com.hoolix.processor.filters

import com.hoolix.processor.models.events.{Event, IntermediateEvent, IntermediatePreviewEvent}
import com.hoolix.processor.utils.Utils

import scala.collection.JavaConversions

case class KVFilter(targetField: String, delimiter: String="\\s+", subDelimiter: String="=") extends Filter {

  override def handle(event: Event): Event = {
    val payload = event.toPayload

    if (payload.contains(targetField)) {

      val field_value = payload.get(targetField).asInstanceOf[Some[String]].get
      if (field_value != null && field_value != "") {

        val words = field_value.split(delimiter)
        if (subDelimiter != "") {
          val kvs = collection.mutable.Map[String, String]()
          words.foreach { word => {
            val kv = word.split(subDelimiter, 2)
            kv.size match {
              case 0 => None
              case 1 => None
              case 2 => kvs.put(kv(0), kv(1)) // TODO prefix or nest
            }
          }}
          if (kvs.nonEmpty) {
            payload.put(targetField + "_map", JavaConversions.mutableMapAsJavaMap(kvs))
          }

        }

      }
    }
    IntermediateEvent(payload)
  }

  override def handle_preview(event: Event): Event = {
    val payload = event.asInstanceOf[IntermediatePreviewEvent].toPayload
    val highlights = event.asInstanceOf[IntermediatePreviewEvent].highlights
    val (start: Int, end: Int) = highlights(targetField).asInstanceOf[(Int, Int)]
    val field_value: String = payload(targetField).asInstanceOf[String]

    val payloadkvs = collection.mutable.Map[String, String]()
    val highlightskvs = collection.mutable.Map[String, (Int, Int)]()

//    val (start, end, field_value) = ctx.get_preview(cfg.target, (-1, -1, ""))
    if (field_value == null || field_value == "") {
      None
    } else {
      Utils.split_with_position(field_value, delimiter).flatMap { case (start, _, kv) => {
        if (kv == "") {
          None
        }
        else {
          val triple: Seq[(Int,Int,String)] = Utils.split_with_position(kv, subDelimiter, 2).toSeq
          if (triple.size == 1) {
            None
//            Some((triple(0)._3, (start + triple(0)._1, start + triple(0)._2, triple(0)._3)))
          }
          else {
            val (_, _, name) = triple(0)
            val (value_start, value_end, value) = triple(1)
            payloadkvs.put(name, value)
            highlightskvs.put(name, (start + value_start, start + value_end))
//            Some((name, (start + value_start, start + value_end, value)))
          }
        }
      }}
    }
    payload.put(targetField + "_map", JavaConversions.mutableMapAsJavaMap(payloadkvs))
    highlights.put(targetField + "_map", JavaConversions.mutableMapAsJavaMap(highlightskvs))
    IntermediatePreviewEvent(highlights, payload)
  }
}

