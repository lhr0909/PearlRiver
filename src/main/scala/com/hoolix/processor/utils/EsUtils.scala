package com.hoolix.pipeline.output.es

import java.io.FileNotFoundException

import com.hoolix.pipeline.core.{Config, Context, PipelineTypeConfig}
import com.hoolix.pipeline.util.TimeRotationUtil
import com.hoolix.pipeline.util.{ESMappingBuilder, TimeRotationUtil, Utils}
import org.elasticsearch.ResourceAlreadyExistsException
import org.elasticsearch.action.admin.indices.alias.{IndicesAliasesAction, IndicesAliasesRequestBuilder}
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.TimeValue
import org.joda.time.DateTime

import scala.io.Source

object EsUtils {

  /**
    * TODO evict indices cache
    *
    * @return index name
    */
  def checkIndexOrCreate(ctx:Context, client:TransportClient, index_name_prefix:String, index_type:String, timeout:TimeValue) : Option[String] = {
    val existIndicesCache = ctx.indices_cache
    //create different index for different type
    val index_name = index_name_prefix + "." + index_type

    if (existIndicesCache.containsKey(index_name))
      return Some(index_name)

    isIndexExist(client, index_name, timeout) match {
      case Some(true) =>
        existIndicesCache.put(index_name, true)
        return Some(index_name)
      case _ =>
    }

    val field_types = ctx.filters match {
      case null => Seq()
      case _    => ctx.filters.map(_._2).flatMap(_.cfg.field_types)
    }

    createIndex(ctx.config, ctx.type_config, client, index_name, timeout, field_types) match {
      case true  =>
        createAlias(client, index_name, index_name_prefix, timeout)
        Some(index_name)
      case false =>
        None
    }
  }

  def isIndexExist(client:TransportClient, index_name:String, timeout:TimeValue) : Option[Boolean] = {
    try {
      val resp = client.admin().indices().prepareExists(index_name).get(timeout)
      Some(resp.isExists)
    } catch {
      case e:Exception => println("error while check index is exist "+e)
        None
    }
  }

  def createAlias(client:TransportClient, index_name:String, alias_name:String, timeout:TimeValue) : Boolean = {
    //add index to alias
    try {
      new IndicesAliasesRequestBuilder(client, IndicesAliasesAction.INSTANCE)
        .addAlias(index_name, alias_name)
        .get(timeout)
      true
    } catch {
      case e =>
        println(s"create alias $alias_name for $index_name failed:"+e.getMessage)
        false
    }
  }
  def createIndex(config:Config, type_config: Option[PipelineTypeConfig],  client:TransportClient, index_name:String, timeout:TimeValue, field_types:Seq[(String,String)] = Seq()) : Boolean = {
    val setting_builder = Settings.builder()

    config.es_index_settings.foreach { case(name, value) =>
      setting_builder.put(name, value)
    }

    val mapping_file = type_config match {
      case Some(cfg) => cfg.mapping_file.getOrElse(config.spark_xyz_conf_file_es_mapping)
      case None      => config.spark_xyz_conf_file_es_mapping
    }

    val mapping_template = Source.fromURL(Utils.resolve_file(mapping_file, config.spark_xyz_conf_file_resolver_list)).mkString

    //add field types to mapping
    val mapping = field_types.isEmpty match {
      case true =>
        mapping_template
      case false => {
        val mapping_builder = ESMappingBuilder(mapping_template)
        field_types.foreach { case (name, typ) =>
          mapping_builder.add_property(name, typ)
        }
        mapping_builder.build()
      }
    }

    return createIndex(
      index_name,
      setting_builder.build(),
      mapping,
      client,
      timeout
    )
  }

  def createIndex(index_name:String, setting:Settings, mapping:String, client:TransportClient, timeout:TimeValue) : Boolean = {
    try{
      val resp = client.admin().indices()
        .prepareCreate(index_name)
        .setSettings(setting)
        .addMapping("_default_", mapping)
        .get(timeout)
      true
    } catch {
      case e: ResourceAlreadyExistsException =>
        true
      case e: FileNotFoundException =>
        throw e
      case e: Throwable => e.printStackTrace()
        false
    }
  }

  def getRotateIndexName(prefix:String, record_timestamp:DateTime, upload_timestamp:DateTime,record_rotate_interval:String, upload_rotate_interval:String) = {
    val record_mark = TimeRotationUtil.getTimeRotationTimestamp(record_timestamp, record_rotate_interval)
    val upload_mark = TimeRotationUtil.getTimeRotationTimestamp(upload_timestamp, upload_rotate_interval)
    prefix + "." + record_mark + "." + upload_mark
  }

  def escape_for_index(str:String, replace:Char = '-'): String = {
    str.toCharArray.map(c =>
      if (Character.isLetter(c) || Character.isDigit(c) || Seq("_", "-").contains(c)) {
        c
      }
      else {
        replace
      }
    ).mkString("").toLowerCase
  }
}
