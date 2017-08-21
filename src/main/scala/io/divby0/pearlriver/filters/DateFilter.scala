package io.divby0.pearlriver.filters

import java.util.Locale

import io.divby0.pearlriver.filters.DateFilter.DateTimeFormatterSettings
import io.divby0.pearlriver.models.{Event, IntermediateEvent}
import org.joda.time.chrono.ISOChronology
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.joda.time.{DateTime, DateTimeZone}

import scala.util.{Failure, Success, Try}

object DateFilter {
  type DateTimeFormatterSettings = (String, String, String) //pattern, locale, timezone

  def dateTimeFormatter(dateTimeFormatterSettings: DateTimeFormatterSettings): DateTimeFormatter = {
    val (pattern, locale, timezone) = dateTimeFormatterSettings
    DateTimeFormat.forPattern(pattern)
      .withLocale(Locale.forLanguageTag(locale))
      .withZone(DateTimeZone.forID(timezone))
      .withChronology(ISOChronology.getInstanceUTC)
  }
}
case class DateFilter(targetField: String,
                      dateTimeFormatterSettings: Seq[DateTimeFormatterSettings] = Seq(("yyyy-MM-dd'T'HH:mm:ss.SSSZ", "en", "UTC"))
                     ) extends Filter {

  //TODO sort timestamp format
  def parseTimestampString(timestamp_str: String): Try[DateTime] = Try(
    // TODO multi datetime formats
    DateFilter.dateTimeFormatter(dateTimeFormatterSettings(0))
      .parseDateTime(timestamp_str)
  )

  override def handle(event: Event): Event = {
    val payload = event.toPayload

    val parsedTimestamp = payload.get(targetField) match {
      case Some(t: String) =>
        parseTimestampString(t) match {
          case Success(dt: DateTime) => dt.getMillis
          case Failure(e) =>
            println("error parsing datetime - " + e.getMessage)
            DateTime.now.getMillis
        }
      case _ =>
        println(s"timestamp field $targetField not found")
        DateTime.now.getMillis
    }

    payload.put("event_timestamp", parsedTimestamp)
    IntermediateEvent(payload)
  }
}
