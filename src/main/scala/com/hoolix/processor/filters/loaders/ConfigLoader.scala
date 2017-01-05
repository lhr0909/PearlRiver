package com.hoolix.processor.filters.loaders

import com.avaje.ebean.{Ebean, RawSqlBuilder}
import com.hoolix.processor.configloaders.Config
import com.hoolix.processor.filters.Filter
import com.hoolix.processor.models.PipelineDBConfig
import com.hoolix.processor.utils.{Converter, EbeanUtil}

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

    try {
      ConfigEntryBuilder.build_from_raw(typ, raw_config)
    }catch {
      case e:ConfigEntryCheckFailException =>
        throw e

      case e =>
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


  /**
    * load all
    */
  def load_from_database(conf:Config) : ConfigPipeline = {

    EbeanUtil.configure(conf)

    val latest_configs = load_filters_from_database(conf)
    latest_configs.foreach( config => {
      println(config.id)
    })
    load_from_db_config(latest_configs)
  }

  /***
    *
    * @param conf
    * @param user_id        default all user
    * @param message_type   default all type
    * @param version        default latest version
    * @return
    */
  def load_from_database(conf:Config, user_id:String = "", message_type:String="", version:Long = -1, config_id:String = "") : ConfigPipeline = {

    EbeanUtil.configure(conf)

    logger.info(s"load pipeline from database user_id:$user_id type:$message_type version:$version")

    var expression = PipelineDBConfig.find.where()

    if (StringUtils.isNotBlank(user_id)) {
      expression = expression.eq("user_id", user_id)
    }

    if (!StringUtils.isBlank(config_id)) {
      expression = expression.eq("id", config_id)

    }
    else {
      if (StringUtils.isNotBlank(message_type)) {
        expression = expression.eq("message_type", message_type)
      }

      if (version != -1) {
        expression = expression.eq("version", version)
      }
    }

    val all_configs = expression.findList()

    load_from_db_config(all_configs)
  }

  /***
    * is_delete is false
    * version > 0
    */

  lazy private val latest_version_config_sql = RawSqlBuilder.parse(
    """select message_type, max(version)
      | from pipeline_config
      | where is_delete = false and version > 0
      | group by message_type
    """.stripMargin)
    .columnMapping("message_type", "message_type")
    .columnMapping("max(version)", "version")
    .create()

  def load_filters_from_database(conf:Config) : Seq[PipelineDBConfig] = {
    EbeanUtil.configure(conf)
    //val latest_configs = PipelineDBConfig.find.setRawSql(latest_version_config_sql).findList()
    val latest_configs = PipelineDBConfig.find.where().in(
      "(message_type, version)",
      Ebean.createQuery(classOf[PipelineDBConfig]) .setRawSql(latest_version_config_sql).where().query()
    ).findList()

    latest_configs
  }

  def load_from_db_config(all_configs: Seq[PipelineDBConfig]) = {

    logger.info(s"loaded pipeline config: "+all_configs.mkString("\n"))

    val latest_configs = all_configs
      //group by token
      .groupBy(_.user_id)
      .flatMap { case (user_id, entries) => {
        //group by type
        entries.groupBy(_.message_type).map { case (message_type, entries_each_type) =>
          //use largest version for each token/type
          entries_each_type.maxBy(_.version)
        }
      }}
    //database config line to RawConfigEntry:

    val raw_config_list = latest_configs.flatMap { config   => config.entries.map { entry =>
      val jsonWriter = new ObjectMapper()

      val source_option : Map[String,Any] = entry.source_option match {
        case null | "" => Map()
        case s         => Converter.java_any_to_scala(new Yaml().load(s)).asInstanceOf[Map[String,Any]]
      }

      RawConfigEntry(
        token       = config.user_id  ,
        name        = entry.name,
        pool        = source_option.getOrElse("pool", "global").toString,
        config_type = entry.`type`,
        types       = Seq(config.message_type),
        target      = entry.source_field,
        field       = jsonWriter.writeValueAsString(entry.target_option),
        require     = jsonWriter.writeValueAsString(entry.requires),
        args        = entry.argument match {
          case null => ""
          case _    => Converter.java_any_to_scala(new Yaml().load(entry.argument))
        },
        orders      = entry.orders.toString,
        version     = config.version
      )
    }
    }.toSeq

    parse_raw_config_list(raw_config_list)
  }

  implicit val formats = DefaultFormats

  def load_from_json(json_str:String) = {
    val root = JsonMethods.parse(json_str)

    println(json_str)
    val configs = root.children
      .map { case node => {
        val jsonWriter = new ObjectMapper()

        val config_type = (node \ "type").extractOrElse("")
        if (StringUtils.isBlank(config_type))
          throw new ConfigCheckFailException("config_type is required to parse")

        try {
          RawConfigEntry(
            token = (node \ "user_id").extractOrElse("*"),
            name = (node \ "name").extractOrElse(""),
            pool = (node \ "source_option" \ "pool").extractOrElse(null),
            config_type = (node \ "type").extractOrElse(""),
            record_type = (node \ "message_type").extractOrElse(""),
            types = Seq((node \ "message_type").extractOrElse("")),
            target = (node \ "source_field").extractOrElse(""),
            field = jsonWriter.writeValueAsString((node \ "target_option").extractOrElse(null)),
            require = jsonWriter.writeValueAsString((node \ "requires").extractOrElse(null)),
            args = Converter.java_any_to_scala(new Yaml().load((node \ "argument").extractOrElse("")))
          )
        } catch {
          case e =>
            e.printStackTrace()
            throw new ConfigCheckFailException(s"error while load config [$config_type]")
        }
      }
      }
    parse_raw_config_list(configs)
  }
}
