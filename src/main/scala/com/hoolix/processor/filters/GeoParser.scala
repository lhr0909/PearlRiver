package com.hoolix.processor.filters

import java.net.InetAddress

import com.hoolix.processor.models.events.{Event, IntermediateEvent}
import com.hoolix.processor.utils.Utils
import com.maxmind.db.CHMCache
import com.maxmind.geoip2.DatabaseReader
import com.maxmind.geoip2.model.CityResponse

import scala.collection.mutable

case class GeoParser(targetField: String, geofile:String = "") extends Filter{
  lazy val reader: DatabaseReader = new DatabaseReader.Builder(Utils.resolve_file(geofile, Seq()).openStream())
    .withCache(new CHMCache()).build()

  def parseResp(ip:String): CityResponse = {
    val address = InetAddress.getByName(ip)
    reader.city(address)
  }

  def parse(ip:String, result:mutable.Map[String,String]): Boolean = {
    ip match {
      case null => false
      case _    => parseResp(ip) match {
      case null => false
      case resp =>
        result += "city"      -> resp.getCity.getName
        result += "country"   -> resp.getCountry.getName
        result += "continent" -> resp.getContinent.getName
        true
      }
    }
  }

  override def handle(event: Event): Event = {
    val payload = event.toPayload
    val ip = payload.get(targetField).asInstanceOf[Some[String]].get
    if (ip != "127.0.0.1") {
      val resp = parseResp(ip)
      payload.put("city", resp.getCity.getName)
      payload.put("country", resp.getCountry.getName)
      payload.put("continent", resp.getContinent.getName)
    }
    IntermediateEvent(payload)
  }
}
