//package com.hoolix.processor.configuration
//
//import java.net.InetAddress
//
//import com.hoolix.processor.utils.{TimeRotationUtil, Utils}
//import org.apache.commons.lang3.StringUtils
//import org.slf4j.LoggerFactory
//
//import scala.io.Source
//
//
///**
//  * 配置项和默认值, 和spark_defaults.conf中的配置一一对应
//  */
//case class Config() {
//  //spark.input.kafka.开头的配置直接加载进kafka
//
//  lazy val logger = LoggerFactory.getLogger(this.getClass)
//
//  /** kafka related config */
//  /**
//    * kafka data decoder, available decoders:
//    * - json
//    * - xyz_line
//    */
//  var spark_xyz_kafka_decoder       = "json"
//
//  var spark_xyz_listen_port           = 8091
//  var spark_xyz_preview_listen_port   = -1
//
//  /* "file" or "database" */
//  var spark_xyz_config_source       = "file"
//  /* when use "database", update interval */
//  var spark_xyz_config_reload_interval_sec  = 10
//
//  var spark_xyz_zookeeper_root      = "/hoolix/address"
//
//  var spark_input_kafka_topics        = "test-topic:3"
//  var spark_input_kafka_group_id      = ""
//
//  /* topic_1, partition_num */
//  var spark_input_kafka_topics_list          = Seq[(String,Int)]()
//  /* topic_1->partition_1, topic_1->partition_2*/
//  var spark_input_kafka_topic_and_partition  = Seq[(String,Int)]()
//
//  var spark_input_kafka_topics_decoder= ""
//
//  //用于生成es的id, 防止offset重置造成的重复id, 每次kafka清空都应该修改该值
//  var spark_input_kafka_version       = "v0"
//  var spark_input_kafka_max_receivers = "2"
//  var spark_input_kafka_repartition_count = "0"
//
//  /** decoder related config */
//  var spark_xyz_raw_decoder_default_token     = "token"
//  var spark_xyz_raw_decoder_default_type      = "*"
//  var spark_xyz_raw_decoder_default_tag       = "_"
//  //TODO map raw decoder
//
//  /** elasticsearch related config */
//  var spark_output_es_cluster         = "elasticsearch"
//  var spark_output_es_servers         = "localhost:9300"
//
//  var spark_output_es_index_record_rotate_interval   = "24hr"
//  var spark_output_es_index_upload_rotate_interval   = "24hr"
//
//  var es_index_settings        = Map[String,String]()
//  var es_index_settings_prefix = "spark.output.es.settings."
//
//  var spark_output_es_batch_count         = "1000"
//  var spark_output_es_batch_size_mb       = "10"
//  var spark_output_es_batch_flush_second  = "3"
//  var spark_output_es_batch_concurrent    = "6"
//
//  var spark_output_es_batch_max_count        = "3000"
//  var spark_output_es_batch_max_backoff_init = "100" //ms
//
//  var spark_output_es_pool_max_total = Runtime.getRuntime.availableProcessors() * 4
//  var spark_output_es_pool_min_idle  = Runtime.getRuntime.availableProcessors() / 2 + 1
//  var spark_output_es_pool_max_idle  = Runtime.getRuntime.availableProcessors()
//
//  var spark_output_es_pool_wait_interval_ms = "1000"
//  var spark_output_es_pool_wait_max_times   = "10"
//
//  var spark_output_es_allow_unknown_token   = "true"
//
//  /** database related config */
//  var spark_xyz_database_driver        = "com.mysql.jdbc.Driver"
//  var spark_xyz_database_url           = "jdbc:mysql://localhost/xyz?characterEncoding=UTF-8"
//  var spark_xyz_database_username      = "root"
//  var spark_xyz_database_password      = "root"
//  var spark_xyz_database_timeout_sec   = "20"
//
//  var spark_xyz_database_cache_reload_delay_ms = "10000"
//
//  /** user usage/limit control config*/
//  var spark_xyz_enable_user_usage      = false
//  var spark_xyz_usage_cache_size       = "2000"
//  var spark_xyz_usage_cache_expire_sec = "20"
//  var spark_xyz_usage_cache_flush_sec  = "10"
//
//  var spark_xyz_enable_hbase = false
//
//  /** monitor related config */
//  var spark_metric_influxdb_host      = ""
//  var spark_metric_influxdb_username  = ""
//  var spark_metric_influxdb_password  = ""
//
//  /** monitor related config */
//  var spark_metric_spark_driver_host = ""
//
//  var spark_metric_xyz_monitor_host  = ""
//  var spark_metric_xyz_monitor_zone  = "xyz"
//  var spark_metric_xyz_monitor_pool_max       = "20"
//  var spark_metric_xyz_monitor_pool_idle_max  = "10"
//  var spark_metric_xyz_monitor_pool_idle_min  = "5"
//
//  var spark_metric_console           = "true"
//
//  var spark_metric_flush_time_ms     = "10000"
//
//  var spark_reckeck_es_query_batch_size   = "50"
//
//  /**
//   * config files
//   */
//  var spark_xyz_conf_file_pipeline   = "pipeline.yml"
//  var spark_xyz_conf_file_es_mapping = "es-default-mapping.json"
//  var spark_xyz_conf_file_geoipdb    = "GeoLite2-City.mmdb"
//  var spark_xyz_conf_file_patterns   = "patterns"
//
//  //http address, ftp address etc. , split by `,`
//  var spark_xyz_conf_file_resolver      = ""
//  var spark_xyz_conf_file_resolver_list:Seq[String] = Seq()
//
//  //Start of TODO options
//  var spark_input_kafka_storage        = ""
//  var spark_input_kafka_input_window   = "10"
//
//
//  var spark_input_hbase_zookeeper_connect = ""
//  var spark_input_hbase_zookeeper_timeout = "120000"
//  var spark_input_hbase_zookeeper_retries = "3"
//
//  var spark_input_hbase_repartition_count = 0 /*0 - auto*/
//  var spark_input_hbase_timeout_ms      = 120000
//  var spark_input_hbase_scan_batch_size = "100"
//  var spark_input_hbase_scan_cache_rows = "100"
//
//  // zookeeper quorum, 创建hbase连接需要用到
//  var spark_input_kafka_zookeeper_connect = "localhost:2181"
//  //END of options
//
//  //origin spark conf
//  var origin = Map[String,String]()
//
//  def dump(): Unit = {
//    println("dump config "+getClass)
//    for (method <- getClass.getMethods) {
//      //getter方法
//      if (method.getName.startsWith("spark_") && !method.getName.contains("$")) {
//        try {
//          println(method.getName + ": " + method.invoke(this))
//        } catch {
//          case e: Throwable => println("Error " + method.getName + " " + e.getMessage)
//            e.printStackTrace()
//        }
//      }
//    }
//  }
//  def after_parse() = {
//
//    if (!StringUtils.isBlank(spark_xyz_conf_file_resolver))
//      spark_xyz_conf_file_resolver_list = spark_xyz_conf_file_resolver.split(",")
//
//    if (!TimeRotationUtil.checkRotationPeriodString(spark_output_es_index_record_rotate_interval)) {
//      println("index record rotation interval invalid, using default 24hr")
//      spark_output_es_index_record_rotate_interval = "24hr"
//    }
//
//    if (!TimeRotationUtil.checkRotationPeriodString(spark_output_es_index_upload_rotate_interval)) {
//      println("index upload rotation interval invalid, using default 24hr")
//      spark_output_es_index_upload_rotate_interval = "24hr"
//    }
//
//    spark_input_kafka_topics_list = spark_input_kafka_topics.split(",")
//      .map(_.split(":"))
//      .map(entry => entry(0) -> entry(1).toInt)
//
//    spark_input_kafka_topic_and_partition = spark_input_kafka_topics_list.flatMap {
//      case (topic, partition_num) => (0 until partition_num).map(topic -> _)
//    }
//  }
//
//  def dump_logger(): Unit = {
//    logger.info("dump config "+getClass)
//    for (method <- getClass.getMethods) {
//      //getter方法
//      if (method.getName.startsWith("spark_") && !method.getName.contains("$")) {
//        try {
//          logger.info(method.getName + ": " + method.invoke(this))
//        } catch {
//          case e => logger.error("Error " + method.getName + " " + e.getMessage)
//            e.printStackTrace()
//        }
//      }
//    }
//  }
//
//  /**
//    * 从spark_defaults.conf加载Config
//    */
//  def build_from(conf:Map[String,String]) = {
//
//    this.es_index_settings = Utils.filterConf(conf, this.es_index_settings_prefix)
//
//    for (method <- getClass.getMethods) {
//
//      if (method.getName.startsWith("spark_") && method.getName.endsWith("_$eq")) {
//        //setter方法
//        val name = method.getName.dropRight("_$eq".length).replace('_', '.')
//        var value :Any = conf.getOrElse(name, null)
//
//        var type_str = ""
//        try {
//
//          if (value != null) {
//            val type_ = method.getParameterTypes.toSeq(0)
//
//            type_str = type_.toString
//
//            value = {
//              if (type_ == classOf[Boolean])
//                value.asInstanceOf[String].toBoolean
//              else if (type_ == classOf[Int] || type_ == classOf[Integer])
//                value.asInstanceOf[String].toInt
//              else if (type_ == classOf[Long])
//                value.asInstanceOf[String].toLong
//              else if (type_ == classOf[Float])
//                value.asInstanceOf[String].toFloat
//              else if (type_ == classOf[Double])
//                value.asInstanceOf[String].toDouble
//              else if (type_ == classOf[Seq[Any]]) {
//                println("seq string")
//                value.asInstanceOf[String].split("\\s+").toSeq
//              }
//              else
//                value
//            }
//            method.invoke(this, value.asInstanceOf[Object])
//          }
//        } catch{
//          case e:Exception =>
//            throw new Exception("Error parse config ["+ name + "] with type [" + type_str  + "]: " + e.getMessage)
//        }
//      }
//    }
//  }
//}
//
//object Config {
//
//  val hbase_row_key_delimiter = ":"
//
//  def from_spark_conf(file:String) = {
//    val spark_conf = Source.fromFile(file).getLines()
//      .filter(line => line != "" && line.trim != "")
//      .filter(!_.startsWith("#"))
//      .map(_.split("\\s+", 2))
//      .map { words => words(0).trim() -> words(1).trim() }
//      .toMap
//
//    Config(spark_conf)
//  }
//
//  def apply(conf:Map[String,String]): Config = {
//    val instance = new Config()
//    instance.origin = conf
//    instance.build_from(conf)
//    instance.after_parse()
//    instance.spark_metric_spark_driver_host = s"http://${InetAddress.getLocalHost.getHostAddress}:${instance.spark_xyz_listen_port}/metric_report"
//    instance
//  }
//
//}
