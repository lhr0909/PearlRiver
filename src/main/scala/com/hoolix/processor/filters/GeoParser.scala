package com.hoolix.processor.filters

import java.net.InetAddress

import com.hoolix.processor.models.{Event, IntermediateEvent}
import com.hoolix.processor.utils.Utils
import com.maxmind.db.CHMCache
import com.maxmind.geoip2.DatabaseReader
import com.maxmind.geoip2.model.CityResponse
import com.hoolix.processor.utils.Utils.{deepGet, deepPut}
import org.slf4j.{Logger, LoggerFactory}

case class GeoParser(
  targetField: String,
  geoFile: String = "",
  cityField: String = "city",
  countryField: String = "country",
  continentField: String = "continent"
) extends Filter {

//  System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "TRACE")

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  lazy val reader: DatabaseReader = new DatabaseReader.Builder(Utils.resolve_file(geoFile, Seq()).openStream())
    .withCache(new CHMCache()).build()

  def parseCityResponse(ip:String): CityResponse = {
    val address = InetAddress.getByName(ip)
    reader.city(address)
  }

  override def handle(event: Event): Event = {
    val payload = event.toPayload
    deepGet(payload, targetField) match {
      case Some(ip: String) => if (ip != "127.0.0.1") {
        val response = parseCityResponse(ip)
        println("println work but logger not work?" + ip)
        logger.info("let us try, this will out the ip synchronously?" + ip)
        logger.error("what if use error let us try, this will out the ip synchronously?" + ip)
        deepPut(payload, cityField, response.getCity.getName)
        deepPut(payload, countryField, response.getCountry.getName)
        deepPut(payload, continentField, response.getContinent.getName)
      }
      case _ =>
    }
    IntermediateEvent(payload)
  }
}
