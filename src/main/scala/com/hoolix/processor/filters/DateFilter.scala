package com.hoolix.processor.filters

import java.text.SimpleDateFormat
import java.util.{Date, Locale, TimeZone}

import com.hoolix.processor.models.{Event, IntermediateEvent}
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.chrono.ISOChronology
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

object DateFilter{
  val dest_format =  {
      DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        .withLocale(Locale.ENGLISH)
        .withZone(DateTimeZone.UTC)
        .withChronology(ISOChronology.getInstanceUTC())
  }
  def now() :String = {
    dest_format.print(DateTime.now())
  }

  def compile_formatter(format:String, zone:String, locale:String) : DateTimeFormatter = {
    DateTimeFormat.forPattern(format)
      .withLocale(Locale.forLanguageTag(locale))
      .withZone(DateTimeZone.forID(zone))
      .withChronology(ISOChronology.getInstanceUTC())   //TODO ?
  }
}
case class DateFilter(targetField: String,
                      formats : Seq[(String,String,String)] = Seq(),
                      dest_format_triple:(String,String,String) = ("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "UTC", "en")
                     ) extends Filter{

  def get_local_timeformat(triple:(String,String,String)) : DateTimeFormatter = {
    get_local_timeformat(triple._1, triple._2, triple._3)
  }
  def get_local_timeformat = DateFilter.compile_formatter(_, _, _)
  //TODO sort by frequence
  lazy val parse_formats = (formats ++ Seq(
    ("dd/MMM/YYYY:HH:mm:ss Z"      , "UTC", "en"),
    ("yyyy/MM/dd HH:mm:ss"         , "UTC", "en"),
    ("yyyy-MM-dd HH:mm:ss"         , "UTC", "en"),
    ("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "UTC", "en"),
    ("yyyy-MM-dd'T'HH:mm:ss.SSSZ"  , "UTC", "en"),
    ("EEE MMM dd HH:mm:ss yyyy"    , "UTC", "en"),
    ("EEE MMM dd HH:mm:ss z yyyy"  , "UTC", "en")
    //java format version: "EEE MMM dd HH:mm:ss zzzz yyyy"
  )).map{ case (format, zone, locale)  => format ->
      get_local_timeformat(format, zone, locale)
  }

  lazy val dest_format = get_local_timeformat(dest_format_triple)

  //TODO sort timestamp format
  def parse(timestamp_str:String): Option[DateTime] = {

    var ret:Option[DateTime] = None
    //java.time.Instant.parse()
    var err = ""
    //TODO use find
    parse_formats.find { case (format_str, format) =>
      try {
        //TODO do without exception
        val date = format.parseDateTime(timestamp_str)
        ret = Some(date)
        //ret = Some(DateFilter.dest_format.get().print(date))
        true
      } catch {
        case e:IllegalArgumentException => err += e + "";false
      }
    }

    if (ret.isEmpty) {
      if ((timestamp_str forall Character.isDigit) &&
        (timestamp_str.length == 10 || timestamp_str.length == 13)
      ) {
        val date = new DateTime(timestamp_str.toLong * math.pow(10, 13 - timestamp_str.length))
        ret = Some(date)
      } else {
        //TODO debug only, remove later
//        println(err)
      }
    }

    ret
  }

  override def handle(event: Event): Event = {
    val payload = event.toPayload
    payload.put("event_timestamp", parse(payload.get(targetField).asInstanceOf[Some[String]].get))
//    println("in date filter")
//    println(payload)
    IntermediateEvent(payload)



//    if ( )
//
//
//    //convert timestamp to @timestamp
//    payload.get(targetField) match {
//      case Some(message_timestamp) =>  parse(message_timestamp.trim) match {
//        case Some(datetime) =>
//          ctx.record_timestamp = datetime
//          Right(
//            Seq(
//              ("@timestamp", DateFilter.dest_format.print(datetime)),
//              ("timestamp", dest_format.print(datetime))
//            )
//          )
//        case None =>
//          ctx.metric(MetricTypes.metric_timestamp_fail)
//          logger.warn("unknown timestamp: "+ message_timestamp + formats)
//          Left(new Exception("unknown timestamp: "+ message_timestamp + formats))
//      }
//      case None => Left(null)
//    }
  }
}
