package com.hoolix.processor.filters


import com.hoolix.processor.models.{Event, IntermediateEvent}

case class KVFilter(targetField: String, delimiter: String="\\s+", subDelimiter: String="=") extends Filter {

  override def handle(event: Event): Event = {
    val payload = event.toPayload

    if (subDelimiter != "") {
      val field_value = payload.get(targetField).asInstanceOf[Some[String]].get
      if (field_value != null && field_value != "") {
        val words = field_value.split(delimiter)

        words.foreach { word => {
          val kvs = word.split(subDelimiter, 2)
          kvs.size match {
            case 0 => None
            case 1 => None
            case 2 => payload.put(kvs(0), kvs(1)) // TODO prefix or nest
          }
        }}
      }
    }
    println("in kv filter")
    println(payload)
    IntermediateEvent(payload)
  }

//  override def handle_preview(ctx: PreviewContext): Either[Throwable, Iterable[(String,(Int,Int, Any))]] = {
//    val (start, end, field_value) = ctx.get_preview(cfg.target, (-1, -1, ""))
//    if (field_value == null || field_value == "")
//      return Left(null)
//
//    Right(
//      Utils.split_with_position(field_value, delimiter).flatMap { case (start, _, kv) => {
//        if (kv == "") {
//          None
//        }
//        else {
//          val triple = Utils.split_with_position(kv, subDelimiter, 2).toSeq
//          if (triple.size == 1) {
//            Some((triple(0)._3, (start + triple(0)._1, start + triple(0)._2, triple(0)._3)))
//          }
//          else {
//            val (_, _, name) = triple(0)
//            val (value_start, value_end, value) = triple(1)
//            Some((name, (start + value_start, start + value_end, value)))
//          }
//        }
//      }}
//    )
//  }
}

