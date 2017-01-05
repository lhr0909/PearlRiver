package com.hoolix.processor.utils
import javax.persistence.PersistenceException

import com.avaje.ebean.{Ebean, EbeanServer, EbeanServerFactory}
import com.avaje.ebean.config.ServerConfig
import com.hoolix.pipeline.bean.{OfflineJob, PipelineDBConfig, PipelineDBConfigEntry}
import com.hoolix.pipeline.core.Config
import org.slf4j.LoggerFactory

object EbeanUtil {

  //TODO init automatically
  val ebean_classes = Seq(
    classOf[PipelineDBConfig],
    classOf[PipelineDBConfigEntry],
    classOf[OfflineJob]
  )

  lazy val logger = LoggerFactory.getLogger(this.getClass)

  def configure(conf:Config): Unit = {
    configure(
      conf.spark_xyz_database_driver,
      conf.spark_xyz_database_url,
      conf.spark_xyz_database_username,
      conf.spark_xyz_database_password
    )
  }
  def configure(driver:String, url:String, username:String, password:String): EbeanServer = {


    implicit def map2Properties(map:Map[String,String]):java.util.Properties = {
      (new java.util.Properties /: map) {case (props, (k,v)) => props.put(k,v); props}
    }

    logger.info(s"configure ebean server $url $username $password")

    try {
       Ebean.getDefaultServer match {
         case server => return server
         case null =>
       }
    }catch {
      case e:PersistenceException => {
        //not init yet
      }
    }

    val config = new ServerConfig()
    config.setName("default")
    config.loadFromProperties(Map(
      "datasource.default.databaseDriver" -> driver,
      "datasource.default.databaseUrl"    -> url,
      "datasource.default.username"       -> username,
      "datasource.default.password"       -> password
    ))
    ebean_classes.foreach(config.addClass(_))
    config.setDefaultServer(true)

    EbeanServerFactory.create(config)
  }

}
