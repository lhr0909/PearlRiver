package com.hoolix.processor.filters

import com.hoolix.processor.models.{Event, IntermediateEvent}

case class SplitFilter(targetField: String, delimiter: String, limit: Int = -1, names: Seq[String] = Seq()) extends Filter {
  override def handle(event: Event): Event = {
//    println("in split filter")
//    println(event)
    // TODO 如果解析失败
    val payload = event.toPayload
    if (targetField != null && targetField != "") {
      if (payload.contains(targetField)) {
        val targetValue = payload.get(targetField).asInstanceOf[Some[String]].get
        if (targetValue != "") {
          val values = targetValue.split(delimiter, limit)
//          println(values)
          val pairs = names.size match {
            case 0 => values.zipWithIndex.map{ case (value, index) => (String.valueOf(index), value)}.toMap
            case _ => names.zip(values) // TODO 如果 names 与 values 的长度不同
          }
//          println(pairs)
          pairs.foreach({
            case (name, value) => payload.put(name, value)
          })

        }
      }
    }

//    println(payload)
    IntermediateEvent(payload)
  }
}
