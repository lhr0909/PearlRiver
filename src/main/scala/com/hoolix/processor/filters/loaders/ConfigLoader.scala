package com.hoolix.processor.filters.loaders

//import com.avaje.ebean.{Ebean, RawSqlBuilder}
//import com.hoolix.processor.configuration.Config
//import com.hoolix.processor.filters.Filter
//import com.hoolix.processor.models.PipelineDBConfig
import com.hoolix.processor.utils.Converter
import java.util.regex.Pattern

import com.hoolix.processor.filters._
import com.hoolix.processor.flows.FilterFlow.FilterMatchingRule
import com.hoolix.processor.models.events.Event
import com.hoolix.processor.utils.Converter
import org.apache.commons.lang3.StringEscapeUtils

/**
  * Hoolix 2017
  * Created by peiyuchao on 1/3/17.
  */
//trait ConfigLoader {
//  // load configurations,
//  def load(): Seq[Filter]
//
//
//}




import java.io.FileInputStream

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.lang3.StringUtils
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods
import org.slf4j.LoggerFactory
import org.yaml.snakeyaml.Yaml

import scala.collection.JavaConversions._

case class RawConfigEntry(token  : String,
                          name   : String, //same as typ if not set
                          pool   : String, //variable pool, default global
                          config_type    : String,
                          record_type    : String = "*",
                          types   : Seq[String] = Seq(), //match types
                          target : String = "", //json string
                          require: String = "", //json string
                          args   : Any    = None, //json string
                          field  : String = "", //json String
                          orders : String = "",
                          version: Long   = 0
                         )

object ConfigLoader  {

  def build_from_local(filename: String): Map[String, Map[String, Seq[FilterMatchingRule]]] = {
    val configs = load_from_yaml(filename)
    build_filter(configs)
  }

  lazy val logger = LoggerFactory.getLogger(this.getClass)

  def parse_raw_config_list(raw_configs: Seq[RawConfigEntry]) = {
    //group by type
    val config_tree = raw_configs.groupBy(_.token).map { case (token,configs) => token ->
      configs.flatMap { case config =>
        config.types.map { record_type => {
          config.copy(record_type = record_type)
        }}
      }.groupBy(_.record_type)
        .map { case (record_type, configs) =>
          configs.size match {
            case 0 => record_type -> (0L, Seq())
            case _ => record_type -> (configs.get(0).version, configs)
          }
        }
    }
    parse_raw_config_tree(config_tree)
  }


  /**
    *  token -> type -> (version, filters)
    */
  def parse_raw_config_tree(raw_configs: Map[String, Map[String, (Long, Seq[RawConfigEntry])]]) = {
    ConfigPipeline(
      raw_configs.map { case (token, type_configs) =>  token ->
        type_configs.map{ case (typ, (version, configs)) => typ ->
          (version, configs.flatMap(parse_raw_config(_)))
        }
      })
  }

  /*
  def parse_config(raw_configs: Map[String, Seq[RawConfigEntry]]) = {
    ConfigPipeline(
      raw_configs.map { case (token, configs) =>
      token -> configs.flatMap(parse_raw_config(_))
    })
  }
  */


  def parse_raw_config(raw_config: RawConfigEntry) : Option[ConfigEntry] = {
    val typ = raw_config.config_type
    if (typ == null || typ == "") {
      logger.warn("no type found")
    }

    //TODO: move this to use Try()
    try {
      ConfigEntryBuilder.build_from_raw(typ, raw_config)
    }catch {
      case e: ConfigEntryCheckFailException =>
        throw e

      case e: Exception =>
        e.printStackTrace()
        throw new ConfigEntryCheckFailException(typ, raw_config.name, "parse error")
    }
  }


  def load_from_yaml(file:String) = {
    logger.info(s"load pipeline from $file")
    val yaml = new Yaml()
    val configs = yaml.load(new FileInputStream(file))
      .asInstanceOf[java.util.ArrayList[java.util.LinkedHashMap[String,Any]]]
      .map { case m =>
        val jsonWriter = new ObjectMapper()
        RawConfigEntry(
          // TODO
          // 空字符串并不会被视为空值,如果在配置文件中指定token为空串的话,token的值不会被设为*
          token  = String.valueOf(m.getOrDefault("token", "*")),
          name   = String.valueOf(m.getOrDefault("name", "")),
          pool   = String.valueOf(m.getOrDefault("pool", "global")),
          config_type = String.valueOf(m.getOrDefault("type", "")),
          types  = m.getOrDefault("types", null) match {
            case null  => Seq("*")
            case types => types.asInstanceOf[java.util.ArrayList[String]].toSeq
          },
          target = String.valueOf(m.getOrDefault("target", "")),
          field  = jsonWriter.writeValueAsString(m.getOrDefault("field", "")),
          require= jsonWriter.writeValueAsString(m.getOrDefault("require", "")),
          args   = Converter.java_any_to_scala(m.getOrDefault("args", None))
        )
      }
    //configs.foreach(println)

    //val (type_config, user_config) = configs.partition(_.config_type == "config")
    val user_config = configs

    //parse_global_config(global_config)
    parse_raw_config_list(user_config)
  }

  implicit val formats = DefaultFormats


  private def build_require(entry : ConfigEntry) = {
    // 如果require为null
    entry.require.conditions.map { case condition =>
      condition.op match {
        case ConditionOp.Exist    => (event: Event) => { event.toPayload.contains(condition.key) } //TODO no pool can not handle x.y.z
        case ConditionOp.NotExist => (event: Event) => { !event.toPayload.contains(condition.key) }
        case ConditionOp.Equal    => (event: Event) => { event.toPayload.getOrElse(condition.key, "") == condition.value }
        case ConditionOp.NotEqual => (event: Event) => { event.toPayload.getOrElse(condition.key, "") != condition.value }
        case ConditionOp.Match    => (event: Event) => { event.toPayload(condition.key).asInstanceOf[String].matches(condition.value) }
        case ConditionOp.NotMatch => (event: Event) => { event.toPayload(condition.key).asInstanceOf[String].matches(condition.value) }
        case ConditionOp.Find     => {
          val pat = Pattern.compile(condition.value)
          (event: Event) => { pat.matcher(event.toPayload.getOrElse(condition.key, "").asInstanceOf[CharSequence]).find() }
        }
        case ConditionOp.NotFind     => {
          val pat = Pattern.compile(condition.value)
          (event: Event) => { !pat.matcher(event.toPayload.getOrElse(condition.key, "").asInstanceOf[CharSequence]).find() }
        }
        case ConditionOp.True     => (event: Event) => { true }
        case ConditionOp.False    => (event: Event) => { false }
      }
    }
  }


  // TODO use config implicit???
  private def build_filter(entry : ConfigEntry) : Filter = {
    //gererate entry name
    val cfg = StringUtils.isBlank(entry.name) match {
      case true =>
        FilterConfig(
          name = entry.`type`,
          pool = entry.pool,
          target = entry.target,
          field_prefix = entry.field_prefix,
          field_types =  entry.field_types
        )
      case false =>
        FilterConfig(
          name = entry.name.trim,
          pool = entry.pool,
          target = entry.target,
          field_prefix = entry.field_prefix,
          field_types =  entry.field_types
        )
    }


    val filter = entry match {
      case entry:PatternConfigEntry   => PatternParser(cfg.target._2, entry.matches, entry.patterns, "conf/patterns") // TODO
      case entry:GeoConfigEntry       => GeoParser(cfg.target._2, "conf/GeoLite2-City.mmdb") //TODO
      case entry:TimestampConfigEntry => DateFilter(cfg.target._2, entry.from_formats)
      case entry:KVConfigEntry        => KVFilter(cfg.target._2, entry.delimiter, entry.sub_delimiter)
      case entry:SplitConfigEntry     => SplitFilter(cfg.target._2, entry.delimiter, entry.max_split, entry.names)
//      case entry:MutateConfigEntry    => MutateFilter(cfg.target._2, entry.cmds)
      case entry:UserAgentConfigEntry => HttpAgentFilter(cfg.target._2)
      case entry:RandomAnomalyDetectionConfigEntry => RandomAnomalyDetectionFilter(entry.percentage, entry.distribution, entry.anomalies)
//      case entry:XmlConfigEntry       => XmlFilter(cfg, entry.paths, entry.max_level)
//      case entry:JsonConfigEntry      => JsonFilter(cfg, entry.paths, entry.max_level)
      case entry:RegexBasedAnomalyDetectionConfigEntry => RegexBasedAnomalyDetectionFilter(entry.params)
      case entry:FilterConfig         => throw new IllegalStateException("config entry has no filter, unreachable code")
      case _ => logger.error("unknown entry: " + entry); null
    }

//    filter.file_resolvers = conf.spark_xyz_conf_file_resolver_list
    filter
  }

  def build_filter(pipe : ConfigPipeline): Map[String, Map[String, Seq[FilterMatchingRule]]] = {

    val filter_map =
      pipe.config.map { case (token, type_entrys) => token -> {
        val each_type = type_entrys.map { case (typ, (version, entrys)) => typ -> {
          val build_entries = entrys.flatMap { entry =>
            Some(build_require(entry) -> build_filter(entry))
          }
          build_entries

        }}
        each_type
      }}


    //TODO remove default map

    filter_map
  }



}

case class FilterConfig(
                         target : (String,String), //variable pool -> field_name
                         name   : String = "", //filter name, should be unique
                         pool   : String = "", //variable pool
                         field_prefix:String = "",
                         field_types:Map[String,String] = Map()
                       )
{

  override def toString: String = {
    s"target:$target; name:$name; pool:$pool; prefix:$field_prefix; types:$field_types"
  }
}
