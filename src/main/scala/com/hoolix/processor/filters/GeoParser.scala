package com.hoolix.processor.filters

import java.io.File
import java.net.InetAddress

import com.hoolix.processor.models.{Event, IntermediateEvent}
import com.hoolix.processor.utils.Utils
import com.maxmind.db.CHMCache
import com.maxmind.geoip2.DatabaseReader

case class GeoParser(targetField: String, geofile:String = "") extends Filter{
//    lazy val reader = new DatabaseReader.Builder(Utils.resolve_file(geofile, file_resolvers).openStream()).withCache(new CHMCache()).build();
  lazy val reader = new DatabaseReader.Builder(Utils.resolve_file(geofile, Seq()).openStream()).withCache(new CHMCache()).build();


  def parseResp(ip:String) = {
        val address = InetAddress.getByName(ip)
        reader.city(address)
    }

    def parse(ip:String, result:scala.collection.mutable.Map[String,String]): Boolean = {
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
      val ip = payload.get(targetField).asInstanceOf[String]

      val resp = parseResp(ip)
      payload.put("city", resp.getCity.getName)
      payload.put("country", resp.getCountry.getName)
      payload.put("continent", resp.getContinent.getName)

      new IntermediateEvent(payload)
//      try {
//        ip match {
//          case null => Left(null)
//          case _ => parseResp(ip) match {
//            case null =>
//              ctx.metric(MetricTypes.metric_geo_fail)
//              Left(new Exception("error"))
//            case resp =>
//              Right(
//                Seq(
//                  ("city", resp.getCity.getName),
//                  ("country", resp.getCountry.getName),
//                  ("continent", resp.getContinent.getName)
//                )
//              )
//          }
//        }
//      } catch {
//        case e : com.maxmind.geoip2.exception.AddressNotFoundException =>
//          Left(e)
//      }
    }
}
