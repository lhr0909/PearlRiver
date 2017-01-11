package com.hoolix.processor.utils
import org.json4s.jackson.Serialization

import scala.collection.JavaConversions._

object Converter {

  implicit val formats = org.json4s.DefaultFormats

  def java_any_to_json_string(item: Any) = {
    if (item == None || item == null)
      None
    else if (item.isInstanceOf[java.util.List[Any]])
      Serialization.write(java_list_to_scala(item.asInstanceOf[java.util.List[Any]]))
    else if (item.isInstanceOf[java.util.Map[Any,Any]])
      Serialization.write(java_map_to_scala(item.asInstanceOf[java.util.Map[String,Any]]))
    else if (item.isInstanceOf[Array[Byte]])
      Serialization.write(new String(item.asInstanceOf[Array[Byte]]))
    else if (item.isInstanceOf[java.lang.String])
      Serialization.write(item.asInstanceOf[String])
    else {
      throw new IllegalArgumentException("unknown type: [" + item.getClass.getName + "] " + item)
    }
  }
  def java_any_to_scala(item: Any) :Any = {
    if (item == None || item == null)
      None
    else if (item.isInstanceOf[java.util.List[Any]])
      java_list_to_scala(item.asInstanceOf[java.util.List[Any]])
    else if (item.isInstanceOf[java.util.Map[Any,Any]])
      java_map_to_scala(item.asInstanceOf[java.util.Map[String,Any]])
    else if (item.isInstanceOf[Array[Byte]])
      new String(item.asInstanceOf[Array[Byte]])
    else if (item.isInstanceOf[java.lang.String])
      item.asInstanceOf[String]
    else {
      throw new IllegalArgumentException("unknown type: [" + item.getClass.getName + "] " + item)
    }
  }
  def java_list_to_scala(list: java.util.List[Any]) : List[Any] = {
    list.map { item =>
      if (item.isInstanceOf[java.util.HashMap[String,Any]])
        java_map_to_scala(item.asInstanceOf[java.util.HashMap[String,Any]])
      //convert byte array to string
      else if (item.isInstanceOf[Array[Byte]])
        new String(item.asInstanceOf[Array[Byte]])
      else if (item.isInstanceOf[java.util.List[Any]])
        java_list_to_scala(item.asInstanceOf[java.util.List[Any]])
      else
        item
    }.toList
  }

  def java_map_to_scala(map: java.util.Map[String,Any]) : Map[String,Any] = {
    map.toMap.map { case (key, value) => key -> {
      if (value.isInstanceOf[java.util.Map[String, Any]])
        java_map_to_scala(value.asInstanceOf[java.util.Map[String, Any]])
      //convert byte array to string
      else if (value.isInstanceOf[Array[Byte]])
        new String(value.asInstanceOf[Array[Byte]])
      else if (value.isInstanceOf[java.util.List[Any]])
        java_list_to_scala(value.asInstanceOf[java.util.List[Any]])
      else
        value
    }}
  }

}
