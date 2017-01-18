package com.hoolix.processor.filters

import java.util.Locale
import com.hoolix.processor.models.events.{Event, IntermediateEvent}
import com.hoolix.processor.filters.DateFilter.DateTimeFormatterSetting
import org.joda.time.chrono.ISOChronology
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.joda.time.{DateTime, DateTimeZone}
import org.slf4j.{Logger, LoggerFactory}
import scala.util.{Failure, Success, Try}
import scala.util.control.Breaks._

object DateFilter {
  type DateTimeFormatterSetting = (String, String, String) // pattern, locale, timezone

  def dateTimeFormatter(dateTimeFormatterSetting: DateTimeFormatterSetting): DateTimeFormatter = {
    val (pattern, locale, timezone) = dateTimeFormatterSetting
    DateTimeFormat.forPattern(pattern)
      .withLocale(Locale.forLanguageTag(locale))
      .withZone(DateTimeZone.forID(timezone))
      .withChronology(ISOChronology.getInstanceUTC)
  }

  val defaultDateTimeFormatterSettings: Seq[DateTimeFormatterSetting] = Seq(
    ("dd/MMM/YYYY:HH:mm:ss Z", "UTC", "en"),
    ("yyyy/MM/dd HH:mm:ss", "UTC", "en"),
    ("yyyy-MM-dd HH:mm:ss", "UTC", "en"),
    ("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "UTC", "en"),
    ("yyyy-MM-dd'T'HH:mm:ss.SSSZ", "UTC", "en"),
    ("EEE MMM dd HH:mm:ss yyyy", "UTC", "en"),
    ("EEE MMM dd HH:mm:ss z yyyy", "UTC", "en")
  )
}

case class DateFilter(
  targetField: String,
  dateTimeFormatterSettings: Seq[DateTimeFormatterSetting] = Seq(("yyyy-MM-dd'T'HH:mm:ss.SSSZ", "en", "UTC"))
) extends Filter {

  lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def parseTimestampString(timestampString: String, dateTimeFormatterSetting: DateTimeFormatterSetting): Try[DateTime] = Try(
    DateFilter.dateTimeFormatter(dateTimeFormatterSetting)
      .parseDateTime(timestampString)
  )

  override def handle(event: Event): Event = {
    val payload = event.toPayload
    var parsedTimestamp: Option[Long] = None
    payload.get(targetField) match {
      case Some(t: String) =>
        breakable {
          for (dateTimeFormatterSetting <- dateTimeFormatterSettings ++ DateFilter.defaultDateTimeFormatterSettings) {
            parseTimestampString(t, dateTimeFormatterSetting) match {
              case Success(dt: DateTime) =>
                parsedTimestamp = Some(dt.getMillis)
                break
              case Failure(e) =>
                logger.error("error parsing datetime - " + e.getMessage)
                parsedTimestamp = Some(DateTime.now.getMillis)
            }
          }
        }
      case _ =>
        logger.error(s"timestamp field $targetField not found")
        parsedTimestamp = Some(DateTime.now.getMillis)
    }
    parsedTimestamp match {
      case Some(value: Long) =>
        payload.put("event_timestamp", value)
        payload.remove(targetField)
      case None =>
    }
    IntermediateEvent(payload)
  }
}
