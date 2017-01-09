package com.hoolix.processor.utils

import java.io._
import java.net.{URI, URL}
import java.nio.charset.StandardCharsets
import java.util.Properties

import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.io.Source

object Utils {

  lazy val logger = LoggerFactory.getLogger(this.getClass)

  def getPropertiesFromFile(filename: String): Map[String, String] = {
    val file = new File(filename)
    require(file.exists(), s"Properties file $file does not exist")
    require(file.isFile(), s"Properties file $file is not a normal file")

    val inReader = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8)
    try {
      val properties = new Properties()
      properties.load(inReader)
      properties.stringPropertyNames().asScala.map(
        k => (k, properties.getProperty(k).trim)).toMap
    } catch {
      case e: IOException =>
        throw e
//        throw new SparkException(s"Failed when loading Spark properties from $filename", e)
    } finally {
      inReader.close()
    }
  }
//  def filterConf(conf: SparkConf,prefix:String) = {
//    //spark.recheck.kafka.out.
//    val prefix_len = prefix.length
//    conf.getAll.filter(_._1.startsWith(prefix)).map{ case(k,v) =>
//        k.drop(prefix_len) -> v
//    }.toMap
//  }

  def filterConf(conf: Map[String,String],prefix:String) = {
    //spark.recheck.kafka.out.
    val prefix_len = prefix.length
    conf.filter(_._1.startsWith(prefix)).map{ case(k,v) =>
      k.drop(prefix_len) -> v
    }
  }

  def parseAddr(addr:String) :(String,Int) = {
    val splits = addr.split(":")
    assert(splits.size == 2)
    splits(0) -> splits(1).toInt
  }

  def splitN(seq: Seq[Int], n:Int) : Seq[Seq[Int]] = {
    seq.size match {
      case 0 => Seq()
      case _ =>
        val (car, cdr) = seq.splitAt(n)
        Seq(car) ++ splitN(cdr, n)
    }
  }

  def getSparkConfLocal(file:String) = {
    Source.fromFile(file).getLines()
      .filter(line => line != "" && line.trim != "")
      .filter(!_.startsWith("#"))
      .map(_.split("\\s+", 2))
      .map { words => words(0).trim() -> words(1).trim() }
      .toMap
  }

  def resolve_file(file:String, resolvers:Seq[String]=Seq()) : URL = {
    val url = resolve_file(file, resolvers, 0)
    logger.info(s"resolve file `$file` with `$url`")
    url
  }
  def resolve_file(file:String, resolvers:Seq[String], i:Int) : URL = {

    var search_list = {
      if (file.startsWith("/")) {
        Seq(file)
      }
      else {
         Seq(file) ++ resolvers.map(_ + "/" + file)
      }
    }
    search_list ++= {
//      try {
//        Seq(SparkFiles.get(file))
//      }catch {
//        case e => Seq()
//      }
      Seq()
    }

    search_list.map(new File(_)).find(_.exists()) match {
      case Some(f)  => return f.toURI.toURL
      case None     =>
    }

    //http://localhost:8000/conf/GeoLite2-City.mmdb

    val loader = this.getClass.getClassLoader

    search_list.map(loader.getResource(_)).find(_ != null) match {
      case Some(addr) => addr
      case None =>
    }

    for (i <- 0 until 2) {
      search_list.find { f => {
        var stream: InputStream = null
        try {
          stream = new URL(f).openStream()
          stream.available() > 0
        } catch {
          case e => false
        }
        finally {
          if (stream != null) {
            try {
              stream.close()
            } catch {
              case e =>
            }
          }
        }
      }
      } match {
        case Some(f) => return new URL(f)
        case None =>
      }
    }

    throw new FileNotFoundException(file + " in " + search_list.mkString("("," ",")"))
  }



  //string utils
  def split_with_position(str:String, delimiter: String, max_split:Int = -1) : Iterable[(Int,Int,String)] = {
    val buffer = scala.collection.mutable.Buffer[(Int,Int,String)]()
    split_with_position(str, delimiter, 0, max_split, buffer);
    buffer
  }

  @tailrec
  private final def split_with_position(str:String, delimiter: String, offset:Int, level:Int, buffer: scala.collection.mutable.Buffer[(Int,Int,String)]) : Unit = {
    if (level == 0) {
      buffer.append((offset, offset+str.length, str))
      return
    }

    val slices = str.split(delimiter, 2)
    if (slices.size == 1) {
      buffer.append((offset, offset+slices(0).length, slices(0)))
      return
    }

    val car = slices(0)
    val cdr = slices(1)

    buffer.append((offset, offset+car.length, car))

    /**
      * find delimiter length
      * >word1   word2
      * >     ^  ^
      */
    val delimiter_match_length = str.substring(car.length).indexOf(cdr)

    assert(delimiter_match_length != -1)

    split_with_position(cdr, delimiter, offset + car.length + delimiter_match_length, level-1, buffer)
  }

}
