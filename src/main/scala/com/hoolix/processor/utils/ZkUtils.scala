package com.hoolix.pipeline.util

import java.net.InetAddress

import com.hoolix.pipeline.core.Config
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkMarshallingError
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.commons.lang3.StringUtils
import org.joda.time.DateTime
import org.json4s.JValue
import org.json4s.jackson.{JsonMethods, Serialization}

import scala.collection.JavaConversions._


case class ZkUtils(host:String, timeout:Int=1000) {
  implicit val formats = org.json4s.DefaultFormats

  val client = new ZkClient(host, timeout, timeout ,ZKStringSerializer)

  def createNodeIfNotExist(path:String) = {
    val real_path = simply_path(path)
    if (!client.exists(real_path)) {
      client.createPersistent(real_path, true)
    }
  }

  def writeJson(path:String, data:Map[String,Any], force:Boolean = true, persist:Boolean=true) = {
    val value = Serialization.write(data)

    val real_path = simply_path(path)

    //delete if exist
    if (client.exists(real_path)) {
      if (force) {
        client.delete(real_path)
      } else {
        throw new Exception("node exist")
      }
    }

    persist match {
      case true  => client.createPersistent(real_path, value)
      case false => client.createEphemeral(real_path, value)
    }
    value
  }

  def readJson(path:String) : Option[JValue] = {
    val data:String = client.readData(path)
    if (StringUtils.isBlank(data)) {
      return None
    }

    try {
      Some(JsonMethods.parse(data))
    }catch {
      case e:Exception =>
        None
    }
  }

  def simply_path(path:String) : String = {
    path.trim.replaceAll("/+", "/").replaceAll("/$", "")
  }

  def readChildJson(path:String) : Seq[JValue] = {
    client.getChildren(path).flatMap { node =>
      readJson(path + "/" + node)
    }
  }

  def readOffsets(consumer_group:String) = {
    val path = s"/consumers/$consumer_group/offsets"
    client.getChildren(path).flatMap { topic =>
      client.getChildren(s"$path/$topic").map { partition =>
        (topic, partition.toInt) -> {
          try {
            val data:String = client.readData(s"$path/$topic/$partition")
            data.toLong
          }catch {
            case e:Exception =>
              e.printStackTrace()
              -1
          }
        }
      }
    }
  }

  def updateOffsets(consumer_group:String, offsets: Iterable[((String, Int), Long)]) = {
    val path = s"/consumers/$consumer_group/offsets"
    offsets.foreach { case ((topic, partition), offset) =>
        client.writeData(s"$path/$topic/$partition", offset.toString)
    }
  }


  def writeAddress(parent:String, path:String, listen_port:Int, persist:Boolean = true) = {
    val data =  Map(
      "addr" -> InetAddress.getLocalHost.getHostAddress,
      "host" -> InetAddress.getLocalHost.getHostName,
      "port" -> listen_port,
      "time" -> new DateTime().toString()
    )

    createNodeIfNotExist(parent)
    writeJson(s"$parent/$path", data, true, persist)
  }

}

object ZKStringSerializer extends ZkSerializer {

  @throws(classOf[ZkMarshallingError])
  def serialize(data : Object) : Array[Byte] = data.asInstanceOf[String].getBytes("UTF-8")

  @throws(classOf[ZkMarshallingError])
  def deserialize(bytes : Array[Byte]) : Object = {
    if (bytes == null)
      null
    else
      new String(bytes, "UTF-8")
  }
}

object ZkUtils {
  def apply(config: Config): ZkUtils = {
    new ZkUtils(config.spark_input_kafka_zookeeper_connect)
  }
}
