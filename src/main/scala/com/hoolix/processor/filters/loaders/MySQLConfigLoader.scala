package com.hoolix.processor.filters.loaders

/**
  * Created by peiyuchao on 2017/1/9.
  */
//class MySQLConfigLoader {
//
//}


/**
  * load all
  */
//  def load_from_database(conf:Config) : ConfigPipeline = {
//
//    EbeanUtil.configure(conf)
//
//    val latest_configs = load_filters_from_database(conf)
//    latest_configs.foreach( config => {
//      println(config.id)
//    })
//    load_from_db_config(latest_configs)
//  }


/***
  *
  * @param conf
  * @param user_id        default all user
  * @param message_type   default all type
  * @param version        default latest version
  * @return
  */
//  def load_from_database(conf:Config, user_id:String = "", message_type:String="", version:Long = -1, config_id:String = "") : ConfigPipeline = {
//
//    EbeanUtil.configure(conf)
//
//    logger.info(s"load pipeline from database user_id:$user_id type:$message_type version:$version")
//
//    var expression = PipelineDBConfig.find.where()
//
//    if (StringUtils.isNotBlank(user_id)) {
//      expression = expression.eq("user_id", user_id)
//    }
//
//    if (!StringUtils.isBlank(config_id)) {
//      expression = expression.eq("id", config_id)
//
//    }
//    else {
//      if (StringUtils.isNotBlank(message_type)) {
//        expression = expression.eq("message_type", message_type)
//      }
//
//      if (version != -1) {
//        expression = expression.eq("version", version)
//      }
//    }
//
//    val all_configs = expression.findList()
//
//    load_from_db_config(all_configs)
//  }

/***
  * is_delete is false
  * version > 0
  */

//  lazy private val latest_version_config_sql = RawSqlBuilder.parse(
//    """select message_type, max(version)
//      | from pipeline_config
//      | where is_delete = false and version > 0
//      | group by message_type
//    """.stripMargin)
//    .columnMapping("message_type", "message_type")
//    .columnMapping("max(version)", "version")
//    .create()
//
//  def load_filters_from_database(conf:Config) : Seq[PipelineDBConfig] = {
//    EbeanUtil.configure(conf)
//    //val latest_configs = PipelineDBConfig.find.setRawSql(latest_version_config_sql).findList()
//    val latest_configs = PipelineDBConfig.find.where().in(
//      "(message_type, version)",
//      Ebean.createQuery(classOf[PipelineDBConfig]) .setRawSql(latest_version_config_sql).where().query()
//    ).findList()
//
//    latest_configs
//  }
//
//  def load_from_db_config(all_configs: Seq[PipelineDBConfig]) = {
//
//    logger.info(s"loaded pipeline config: "+all_configs.mkString("\n"))
//
//    val latest_configs = all_configs
//      //group by token
//      .groupBy(_.user_id)
//      .flatMap { case (user_id, entries) => {
//        //group by type
//        entries.groupBy(_.message_type).map { case (message_type, entries_each_type) =>
//          //use largest version for each token/type
//          entries_each_type.maxBy(_.version)
//        }
//      }}
//    //database config line to RawConfigEntry:
//
//    val raw_config_list = latest_configs.flatMap { config   => config.entries.map { entry =>
//      val jsonWriter = new ObjectMapper()
//
//      val source_option : Map[String,Any] = entry.source_option match {
//        case null | "" => Map()
//        case s         => Converter.java_any_to_scala(new Yaml().load(s)).asInstanceOf[Map[String,Any]]
//      }
//
//      RawConfigEntry(
//        token       = config.user_id  ,
//        name        = entry.name,
//        pool        = source_option.getOrElse("pool", "global").toString,
//        config_type = entry.`type`,
//        types       = Seq(config.message_type),
//        target      = entry.source_field,
//        field       = jsonWriter.writeValueAsString(entry.target_option),
//        require     = jsonWriter.writeValueAsString(entry.requires),
//        args        = entry.argument match {
//          case null => ""
//          case _    => Converter.java_any_to_scala(new Yaml().load(entry.argument))
//        },
//        orders      = entry.orders.toString,
//        version     = config.version
//      )
//    }
//    }.toSeq
//
//    parse_raw_config_list(raw_config_list)
//  }