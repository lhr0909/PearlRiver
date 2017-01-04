package com.hoolix.pipeline.util

import org.json4s.{DefaultFormats, Extraction, JValue}
import org.json4s.JsonAST.JObject
import org.json4s.JsonAST.JNothing
import org.json4s.JsonDSL._
import org.json4s.jackson.{JsonMethods, Serialization}
import org.slf4j.LoggerFactory

import scala.util.parsing.json.JSONObject
import scala.collection.mutable

case class ESMappingBuilder(template_str:String) {

  //throw Exception if parse failed
  val template = JsonMethods.parse(template_str)

  def getOrCreate(map : mutable.Map[String,Any], name:String) = {
    map.getOrElse(name, {
      val m = mutable.Map()
      map.put(name, m)
      m
    }).asInstanceOf[mutable.Map[String, Any]]
  }

  val mappings   = scala.collection.mutable.Map[String, Any]()
  val properties = getOrCreate(mappings, "properties")

  def add_property(name: String) : mutable.Map[String,Any] = {
    val node = getOrCreate(properties, name)
    node.put("doc_values", true)
    node
  }

  def add_property(name: String, `type`: String, cfgs: Map[String,Any] = Map()) : mutable.Map[String,Any] = {
    val node = add_property(name)

    node.put("type", `type`)

    cfgs.foreach { case(k,v) =>
        node.put(k, v)
    }

    node
  }

  def add_property(props: Seq[(String,String, Map[String,Any])]) : Unit = {
    props.foreach { case (name, typ, cfgs) => {
      add_property(name, typ, cfgs)
    }}
  }

  implicit val formats = org.json4s.DefaultFormats

  def build() = {
    val properties_json:JObject = ("properties" -> Extraction.decompose(properties))
    //merge, not override
    val generate = template.merge(properties_json)
    JsonMethods.compact(generate)
  }

}
