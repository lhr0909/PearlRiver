/**
  * Created by peiyuchao on 2017/1/3.
  */

// mirror pipeline configentry for filters

package com.hoolix.processor.filters.loaders

import com.hoolix.processor.configloaders.PipelineTypeConfig
import com.hoolix.processor.filters.DateFilter
import org.apache.commons.lang3.StringUtils
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag


case class ConfigPipeline(config: Map[String,Map[String,(Long, Seq[ConfigEntry])]]) {
  //some global arguments
}

object ConfigEntryBuilder {

  object ConfigEntry {
    val entry_mapping = Map (
      "config"    -> TypeConfigEntry,
      "pattern"   -> PatternConfigEntry,
      "kv"        -> KVConfigEntry,
      "split"     -> SplitConfigEntry,
      "geoip"     -> GeoConfigEntry,
      "timestamp" -> TimestampConfigEntry,
      "mutate"    -> MutateConfigEntry,
      "user-agent"-> UserAgentConfigEntry,
      "json"      -> JsonConfigEntry,
      "xml"       -> XmlConfigEntry,
      "regex-based-anomaly-detection" -> RegexBasedAnomalyDetectionConfigEntry,
      "random-anomaly-detection" -> RandomAnomalyDetectionConfigEntry
    )
  }

  lazy val logger = LoggerFactory.getLogger(this.getClass)

  def build_from_raw(typ:String, raw_config:RawConfigEntry) :Option[ConfigEntry] = {

    val entry = ConfigEntry.entry_mapping.get(raw_config.config_type) match {
      case Some(builder) => builder()
      case None => logger.warn("unknown entry type: " + raw_config.config_type); return None
    }

    entry.`type` = typ
    entry.name  = raw_config.name
    entry.pool  = raw_config.pool

    entry.parse_target(raw_config.target)

    if (raw_config.args != None && raw_config.args != null && raw_config.args != "") {
      var args_err : Option[String] = None

      try {
        args_err = entry.parse_args(raw_config.args)
      } catch {
        case e =>
          e.printStackTrace()
          println("-"*40)
          println(s"parse ${entry.name} error:")
          println("  "+raw_config)
          println(entry.args_usage)
          println("-"*40)
          throw e
      }
      entry.check_args()

      args_err match {
        case Some(err) =>
          println("-"*40)
          println(s"parse ${entry.name} error:")
          println("  "+raw_config)
          println(entry.args_usage)
          println("-"*40)
          throw new IllegalArgumentException(err)
        case None =>
      }
    }

    entry.parse_require(raw_config.require)
    entry.parse_field(raw_config.field)

    Some(entry)
  }

  /*
  def parse_target(target:String) = {
    val words = target.split("\\.", 2)

    words.size match {
      case 1 => ("global", words(0))
      case 2 => (words(0), words(1))
    }
  }
  */

}

//parsed config
trait ConfigEntry {

  var usage      : String = """
                              |type:%s
                            """.stripMargin

  val args_usage : String = ""

  implicit val formats = DefaultFormats

  /***
    * config type
    */
  var `type`  : String = ""
  var name    : String = ""
  var pool    : String = "global"
  //field_group, field_name
  var target  : (String,String) = ("", "")
  var require : Requires = null
  var args    : String = ""

  /***
    * field types
    */
  var field_types : Map[String, String] = Map()

  /***
    * add prefix for each field name
    */
  var field_prefix: String = ""
  //var pre_condition:Condition = Condition(ConditionOp.True)
  //var post_condition:Condition = Condition(ConditionOp.True)

  def parse_args(args:Any) : Option[String] = {None}

  case class CanBeSafeCast[T](value:Any) {
    //def mark = ""
    def safe_cast[T]()(implicit m: ClassTag[T]) : T = {
      safe_cast[T]("")
    }
    def safe_cast[T](mark:String)(implicit m: ClassTag[T]) : T = {
      try {
        return m.runtimeClass.cast(value).asInstanceOf[T]
      }catch {
        case e =>
          e.printStackTrace()
      }
      throw new ConfigEntryCheckFailException(`type`, name, s"expect [$mark] is type of [${m.runtimeClass.getName}] actually [${value.getClass.getName}]")
    }
  }
  implicit def Any2CanBeSmartCast(value:Any) = {
    CanBeSafeCast(value)
  }

  def parse_target(target:String) = {

    if (target != null) {
      val words = target.split("\\.", 2)

      this.target = words.size match {
        case 1 => ("global", target)
        case 2 => (words(0), words(1))
      }
    }
    /*
    if(target != null) {
      this.target = JsonMethods.parse(target).extractOpt[Any] match {
        case Some(field: String) => ("global", field)
        case Some(kv: Map[String, String]) => kv.toSeq(0)
        case None =>("","")
        case any => throw new IllegalArgumentException("unknown target: " + any)
      }
    }
    */
  }
  //pre check formats
  def check_args() = {}

  def parse_require(args:String) = {
    val node = JsonMethods.parse(args)
    require = Requires(node.extractOpt[Seq[String]].getOrElse(Seq()).map{ConditionsBuilder.build_condition(_)})
  }
  def parse_field(field:String) = {
    val node = JsonMethods.parse(field)
    this.field_prefix = (node \ "prefix").extractOrElse("")
    this.field_types  = (node \ "types").extractOpt[Map[String,String]].getOrElse(Map[String,String]())
  }

  override def toString: String = { "name:%s target:%s require:%s args:%s types:%s".format(name, target,require,args, field_types) }
}

case class PatternConfigEntry() extends ConfigEntry {
  var matches:Seq[String] = Seq()
  var patterns:Map[String, String] = Map()

  target = ("global", "clientip")

  override val args_usage: String =
    """|[args usage]
       |  require type: Object
       |[example]
       |```
       |args:
       |  pattern_name : [nginx_access, nginx_error]
       |  patterns :
       |    - nginx_access : "%{NGINX_ACCESS}"
       |```
    """.stripMargin

  override def parse_args(args: Any): Option[String] = {

    val entry = this

    //TODO use reflect
    val node = args.safe_cast[Map[String,Any]]("arguments")
    entry.matches  = node.getOrElse("pattern_name", Seq()).safe_cast[Seq[String]]("arguments.pattern_name")
    val origin_patterns = node.getOrElse("patterns", Seq()).safe_cast[Seq[Map[String, String]]]("arguments.patterns")
    if (origin_patterns.size != 0)
      entry.patterns = origin_patterns.reduce(_++_)

    None
  }

  override def toString() : String = {super.toString + ",matches:%s, patterns:%s".format(matches, patterns)}
}

/**
  * KV
  */
case class KVConfigEntry() extends ConfigEntry {
  var delimiter:String = ""
  var sub_delimiter:String = ""

  override val args_usage: String =
    """|[args usage]
       |  require type: Object
       |[example]
       |```
       |args:
       |  delimiter :  ","
       |  sub_delimiter : "="
       |```
    """.stripMargin

  override def parse_args(args: Any): Option[String] = {
    //timestamps.getClass
    val json = args.safe_cast[Map[String,Any]]("arguments") //JsonMethods.parse(args)
    delimiter = json.getOrElse("delimiter", " ").safe_cast[String]("arguments.delimiter")
    sub_delimiter = json.getOrElse("sub_delimiter", ":").safe_cast[String]("arguments.sub_delimiter")

    None
  }

  override def toString() : String = {super.toString + ",delimiter:%s,sub_delimiter:%s".format(delimiter, sub_delimiter)}
}

case class SplitConfigEntry() extends ConfigEntry {
  var delimiter:String = " "
  var max_split:Int = -1
  var names:Seq[String] = Seq()

  override val args_usage: String =
    """|[args usage]
       |  require type: Object
       |[example]
       |```
       |args:
       |  delimiter :  ","
       |  max_split :  10
       |  names     :
       |     - "field_0"
       |     - "field_1"
       |```
    """.stripMargin


  override def parse_args(args: Any): Option[String] = {
    //timestamps.getClass
    val json = args.safe_cast[Map[String,Any]]("arguments")
    delimiter = json.getOrElse("delimiter", delimiter).toString
    max_split = json.getOrElse("max_split", max_split).toString.toInt
    names     = json.getOrElse("names", names).safe_cast[Seq[Any]]("arguments.names").map(_.toString)

    None
  }

  override def toString() : String = {super.toString + ",delimiter:%s,max_split:%s,names:%s".format(delimiter, max_split, names)}
}

case class GeoConfigEntry() extends ConfigEntry {}

case class TimestampConfigEntry() extends ConfigEntry {
  //timeformat, timezoke, locale

  val to_format_default : (String,String,String) = ("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "UTC", "en")

  var from_formats = Seq[(String,String,String)]()
  var to_format : (String,String,String) = to_format_default


  override val args_usage: String =
    """|[args usage]
       |  require type: Object
       |[example]
       |```
       |args:
       |  from_format :
       |    - ["dd/MMM/YYYY:HH:mm:ss Z", "UTC", "en"]
       |  to_format :
       |    - ["dd/MMM/YYYY:HH:mm:ss.SSSZ", "UTC", "en"]
       |```
    """.stripMargin

  def seq_to_triple(seq:Seq[String]) : Option[(String,String,String)] = {
    {
      seq.size match {
        case 0 => None
        case 1 => Some(seq(0), "UTC", "en")
        case 2 => Some(seq(0), seq(1), "en")
        case 3 => Some(seq(0), seq(1), seq(2))
        case _ => throw new IllegalArgumentException("unknown timestamp format: " + seq)
      }
    } match {
      case Some(tuple) if (StringUtils.isBlank(tuple._1)) => None
      case any =>  any
    }

  }
  override def parse_args(args: Any): Option[String] = {
    //timestamps.getClass
    val node = args.safe_cast[Map[String,Any]]("arguments")

    from_formats = node.getOrElse("from_format", Seq()).safe_cast[Seq[Seq[String]]]("arguments.from_format")
      .flatMap(seq_to_triple(_))

    to_format = seq_to_triple({
      node.get("to_format").safe_cast[Option[Seq[Seq[String]]]]("arguments.to_format") match {
        case None        => Seq("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "UTC", "en")
        case Some(Seq()) => Seq("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "UTC", "en")
        case Some(seq: Seq[String]) => seq(0)
      }
    }).getOrElse(to_format_default)

    None
  }

  override def check_args: Unit = {
    from_formats.foreach { case (format, timezone, lang) =>
      try {
        DateFilter.compile_formatter(format, timezone, lang)
      } catch {
        case e =>
          throw new ConfigCheckFailException(s"compile timestamp [$name] failed: $format $timezone $lang")
      }
    }
  }

  override def toString() : String = {super.toString + ",from:%s,to:%s".format(to_format, to_format)}
}

case class MutateConfigEntry() extends ConfigEntry {
  var raw_cmd = ""
  var cmds: Seq[(String,Seq[Any])] = Seq()

  override val args_usage: String =
    """|[args usage]
       |  require type: List
       |[example]
       |```
       |args:
       |   - create:
       |     - "x" : "test"
       |     - "y" : {"global": "x"}
       |   - insert:
       |     - "x" : "test"
       |     - "test" : {"global": "x"}
       |   - rename:
       |     - "x" : "xx"
       |   - replace:
       |     - "test" : ["^","###### "]
       |   - replace:
       |     - "test" : ["$",{"global": "xx"}]
       |```
    """.stripMargin

  override def parse_args(args: Any): Option[String] = {
    raw_cmd = args.toString

    cmds = args match {
      case None => Seq()
      case _    => args.safe_cast[Seq[Map[String,List[Any]]]]("arguments").flatMap(m => m.toSeq)
    }
    None
  }
  override def toString() : String = {super.toString +",raw:%s,cmd:%s".format(raw_cmd, cmds)}
}

case class UserAgentConfigEntry() extends ConfigEntry {

}

case class RandomAnomalyDetectionConfigEntry() extends ConfigEntry {
  var percentage: Double = 5.0
  var distribution: Seq[Seq[String]] = Seq(Seq("error", "10"), Seq("unknown", "20"), Seq("warn", "30"), Seq("info", "40"))
  var anomalies: Seq[String] = Seq("anomaly_demo")

  override val args_usage: String =
    """|[args usage]
       |  require type: List
       |[example]
       |```
       |args:
       |    - "global.type match ^apache_access|nginx_access$"
       |```
    """.stripMargin

  override def parse_args(args: Any): Option[String] = {
    val json = args.safe_cast[Map[String,Any]]("arguments")
    percentage = json.getOrElse("percentage", percentage).safe_cast[Double]("arguments.percentage")
    distribution = json.getOrElse("distribution", distribution).safe_cast[Seq[Seq[String]]]("arguments.distribution")
    // TODO 判断和是否为100
    anomalies = json.getOrElse("anomalies", anomalies).safe_cast[Seq[String]]("arguments.anomalies")
    None
  }
}
case class JsonConfigEntry() extends ConfigEntry {
  var paths: Seq[String] = Seq("*")
  var max_level:Int = 5

  override val args_usage: String =
    """|[args usage]
       |  require type: Object
       |[example]
       |```
       |args:
       |  paths:
       |    - "field_0"
       |    - "field_1"
       |
       |  max_level: 5 (optional)
       |```
    """.stripMargin

  override def parse_args(args: Any): Option[String] = {
    if (args == None)
      return None
    val config = args.safe_cast[Map[String,Any]]("arguments")

    config.get("paths") match {
      case None =>
      case Some(path) => paths = path.safe_cast[Seq[String]]("arguments.paths")
    }
    max_level = config.get("max_level").getOrElse(max_level).toString.toInt

    None
  }
  override def toString() : String = {super.toString +",paths:%s".format(paths)}
}

case class XmlConfigEntry() extends ConfigEntry {
  var paths: Seq[String] = Seq("*")
  var max_level:Int = 10

  override val args_usage: String =
    """|[args usage]
       |  require type: Object
       |[example]
       |```
       |args:
       |  paths:
       |    - "field_0"
       |    - "field_1"
       |
       |  max_level: 5 (optional)
       |```
    """.stripMargin

  override def parse_args(args: Any): Option[String] = {
    if (args == None)
      return None
    val config = args.safe_cast[Map[String,Any]]("arguments")

    config.get("paths") match {
      case None =>
      case Some(path) => paths = path.safe_cast[Seq[String]]("arguments.paths")
    }
    max_level = config.get("max_level").getOrElse(max_level).toString.toInt

    None
  }
  override def toString() : String = {super.toString +",paths:%s".format(paths)}
}

case class RegexBasedAnomalyDetectionConfigEntry() extends ConfigEntry {

  override val args_usage: String =
    """|[args usage]
       |  require type: Object
       |[example]
       |```
       |args:
       |      params: [
       |        ["errormsg", "(?i).*(exit signal segmentation fault).*", "anomaly_segmentation_fault", "error"],
       |        ["errormsg", "(?i).*((denied by server configuration)|((file does not exist)|(failed to open stream: no such file or directory))).*", "anomaly_denied", "warn"],
       |        ["errormsg", "(?i).*(client sent malformed host header).*", "anomaly_code_red", "error"],
       |        ["errormsg", "(?i).*((authentication failed)|((user .* not found)|(user .* in realm .* not found))|(authentication failure)).*", "anomaly_authentication", "warn"],
       |        ["errormsg", "(?i).*((invalid uri in request)|((file name too long)|(request failed: uri too long))).*", "anomaly_illegal_url", "warn"],
       |      ]
       |```
    """.stripMargin

  var params: Seq[Seq[String]] = Seq()
  override def parse_args(args: Any): Option[String] = {
    val json = args.safe_cast[Map[String,Any]]("arguments")
    params = json.getOrElse("params", params).safe_cast[Seq[Seq[String]]]("arguments.params")
    None
  }
}

case class TypeConfigEntry() extends ConfigEntry {

  var config = PipelineTypeConfig()
  override def parse_args(args: Any): Option[String] = {
    val json = args.safe_cast[Map[String,Any]]("arguments")
    json.getOrElse("mapping", "").safe_cast[String]("arguments.mapping") match {
      case s if s.trim != "" => config.mapping_file = Some(s)
      case _ =>
    }
    None
  }

  override def toString() : String = {super.toString +",config:%s".format(config)}
}
