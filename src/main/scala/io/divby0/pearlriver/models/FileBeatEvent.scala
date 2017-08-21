package io.divby0.pearlriver.models

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.joda.time.DateTimeZone
import org.joda.time.chrono.ISOChronology
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import java.util.Locale

/**
  * Created by peiyuchao on 2017/1/5.
  */
object FileBeatEvent {
  val dateTimeFormatter: DateTimeFormatter = DateTimeFormat
    .forPattern ("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    .withLocale(Locale.ENGLISH)
    .withZone(DateTimeZone.UTC)
    .withChronology(ISOChronology.getInstanceUTC)

  def fromJsonString(json: String): FileBeatEvent = {
    implicit val formats = DefaultFormats
    parse(json).transformField({
      case ("@timestamp", x) => ("timestamp", x)
      case ("input_type", x) => ("inputType", x)
      case ("type", x) => ("_type", x)
    }).extract[FileBeatEvent]
  }
}

case class FileBeatEvent(
                          timestamp: String,
                          beat: Beat,
                          fields: Map[String, String],
                          inputType: String,
                          message: String,
                          offset: Long,
                          source: String,
                          tags: Seq[String],
                          _type: String
                        ) extends Event {

  def toPayload: collection.mutable.Map[String, Any] = collection.mutable.Map(
    "timestamp" -> FileBeatEvent.dateTimeFormatter.parseMillis(timestamp),
    "beat" -> beat,
    "fields" -> fields,
    "inputType" -> inputType,
    "message" -> message,
    "offset" -> offset,
    "source" -> source,
    "tags" -> tags,
    "type" -> _type
  )

  override def indexName = s"${_type}" //add more stuff
  override def indexType = _type
  override def docId = ???
  override def toIndexRequest = ???
}

