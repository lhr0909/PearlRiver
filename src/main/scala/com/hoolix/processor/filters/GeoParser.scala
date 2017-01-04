package com.hoolix.pipeline.filter

import java.io.File
import java.net.InetAddress

import com.hoolix.pipeline.core.{Context, Filter, FilterConfig, MetricTypes}
import com.hoolix.pipeline.util.Utils
import com.maxmind.db.CHMCache
import com.maxmind.geoip2.DatabaseReader

case class GeoParser(cfg:FilterConfig, geofile:String = "") extends Filter{
    lazy val reader = new DatabaseReader.Builder(Utils.resolve_file(geofile, file_resolvers).openStream()).withCache(new CHMCache()).build();

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

  /*
    public static Optional<Tuple3<String,String,String>> getCityCn(String ip) {
      Optional<CityResponse> resp_opt = getGeoResponse(ip);
      if (resp_opt.isPresent()) {
        CityResponse resp = resp_opt.get();

        String name = resp.getCity().getNames().get("zh-CN");
        //如果没有中文名, 查询其他名字
        if (name == null)
          name = resp.getCity().getName();

        //如果没有名字, 经纬度也是瞎扯, 不可信
        if (name == null) {
          Logger.debug("no name for: "+ ip+" " + resp.getLocation().getLatitude()+"," + resp.getLocation().getLatitude());
          return Optional.empty();
        }
        return Optional.of(new Tuple3(name, resp.getLocation().getLongitude() + "", resp.getLocation().getLatitude() + ""));
      }
      return Optional.empty();
    }
  }
  */
    override def handle(ctx: Context): Either[Throwable, Iterable[(String,Any)]] = {

      val ip = ctx.get(cfg.target,null)

      try {
        ip match {
          case null => Left(null)
          case _ => parseResp(ip) match {
            case null =>
              ctx.metric(MetricTypes.metric_geo_fail)
              Left(new Exception("error"))
            case resp =>
              Right(
                Seq(
                  ("city", resp.getCity.getName),
                  ("country", resp.getCountry.getName),
                  ("continent", resp.getContinent.getName)
                )
              )
          }
        }
      } catch {
        case e : com.maxmind.geoip2.exception.AddressNotFoundException =>
          Left(e)
      }
    }
}
