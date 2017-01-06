package com.hoolix.processor.filters

import java.util.regex.Pattern

import com.hoolix.processor.models.{Event, IntermediateEvent}
import com.hoolix.processor.utils.Utils

import scala.collection.mutable
import scala.io.Source

object PatternParser {
  def parser_group_names(pattern:String) : Set[String] = {

    var names = Set[String]()

    val m = Pattern.compile("""\(?<([a-zA-Z][a-zA-Z0-9]*)>""").matcher(pattern)

    while (m.find()) {
      names ++= Set(m.group(1))
    }

    names
  }
}

/**
  * @param matches type不匹配时尝试项
  * @param patterns 自定义pattern, 参考patterns文件
  */
case class PatternParser(targetField: String,
                          matches:Seq[String]=Seq(),
                          patterns:Map[String,String]=Map(),
                          builtin_pattern_file:String = "patterns"
                        ) extends Filter{

  //TODO use shared default pattern
  lazy val grok_pattern_lst = Source.fromURL(Utils.resolve_file(builtin_pattern_file, Seq())).getLines()
    .filter(_.trim != "")
    .filter(!_.startsWith("#")).filter(!_.startsWith("//"))
    .map(_.split("[ \\s\t]+",2))
    .map(words => words(0).trim -> words(1).trim)
    .toMap ++ patterns


  //pattern:name
  def get_group_name(name: String): (String,Option[String]) = {
    val name_pairs = name.split(":")
    val pattern_name = name_pairs(0)
    val group_name:Option[String] = if (name_pairs.length == 1) None else Some(name_pairs(1))

    (pattern_name, group_name)
  }

  //扩展正则, 把grok的语法%{name}扩展成正则语法<?name:>
  //TODO  grok%{}语法转义
  def extend_pattern(str:String, prefix:String="") :String = {

    var dest = ""

    val ref_pattern = "%\\{([^}]+)\\}".r

    var last = 0
    ref_pattern.findAllMatchIn(str).foreach(mat => {
      dest += str.substring(last, mat.start)

      val (pattern_name, group_name_opt) = get_group_name(mat.group(1))

      if (group_name_opt.isEmpty) { //非命名分组
        val subpattern = extend_pattern(grok_pattern_lst(pattern_name), prefix)
        dest += "(?:%s)".format(subpattern)
      }
      else {//命名分组
        val (group_name, sub_prefix) = prefix match {
           case "" => (group_name_opt.get, group_name_opt.get)
           case _  => (prefix+"x"+group_name_opt.get, prefix+"x"+group_name_opt.get)
         }
        val sub_pattern = extend_pattern(grok_pattern_lst(pattern_name), sub_prefix)
        dest += "(?<%s>%s)".format(group_name, sub_pattern)
      }
      last = mat.end
    })
    dest += str.substring(last, str.length)
    dest
  }
  //预编译正则
  //pattern thread-safe
  lazy val compiled_patterns = grok_pattern_lst.map { case (name,pat) =>
    (name, extend_pattern(pat))
  } .map { case (name, pattern_str) =>
    val pattern = pattern_str.getBytes
    val regex = Pattern.compile(pattern_str)
    (name, (PatternParser.parser_group_names(pattern_str), regex))
  }

  /*
  lazy val compiled_patterns_joni = grok_pattern_lst.map { case (name,pat) =>
    (name, extend_pattern(pat))
  }.map{case (name, pattern_str) =>
    val pattern = pattern_str.getBytes
    val regex = new Regex(pattern, 0, pattern.length, org.joni.Option.NONE, UTF8Encoding.INSTANCE);


    (name, (pattern_str, regex))
  }
  */

  def match_byname(pattern_name:String, str_str:String): (Boolean, Iterable[(String,String)]) = {

    val (names, pattern)  = compiled_patterns(pattern_name)
    match_pattern(names, pattern, str_str)

    //joni pattern
    /*
    val (pattern_str, pattern)  = compiled_patterns_joni(pattern_name)
    match_pattern_joni(pattern_str, pattern, str_str)
    */

  }

  def match_pattern(names:Set[String],pattern:Pattern, str:String): (Boolean, Iterable[(String,String)]) = {

    val matcher = pattern.matcher(str)

    val map = mutable.HashMap[String,String]()

    //partial match, not match entire message

    if (matcher.find()) {
      for (name <- names) {
        map.put(name, matcher.group(name))
      }
      (true, map)
    }
    else {
      (false, map)
    }
  }

  def match_pattern_with_position(names:Set[String],pattern:Pattern, str:String): (Boolean, Iterable[(String,(Int, Int, String))]) = {

    val matcher = pattern.matcher(str)

    val map = mutable.HashMap[String,(Int, Int, String)]()

    //partial match, not match entire message

    if (matcher.find()) {
      for (name <- names) {
        map.put(name, (matcher.start(name), matcher.end(name), matcher.group(name)))
      }
      (true, map)
    }
    else {
      (false, map)
    }
  }


  override def handle(event: Event): Event = {
    val payload = event.toPayload
    val types   =  {
      //if matches exist, use matches
      if (matches.size != 0) {
        matches
      }
      //else use type
      else {
//        Seq(ctx.get("type",""))
        Seq(payload.get("type").asInstanceOf[String])
      }
    }

    val message = payload.get(targetField).asInstanceOf[String]

    //先试type, 再试matcher列表
    for (typ <- types) {
      val pattern_opt = compiled_patterns.get(typ)
      if (pattern_opt.isEmpty) {
        //logger.warn("error: pattern not found [" + typ+"]")
//        ctx.metric(MetricTypes.metric_no_pattern)
      }
      else {

        val (names, pattern) = pattern_opt.get

        val (grok_parse_ok, grok_result) = match_pattern(names, pattern, message)

        if (grok_parse_ok) {
          grok_result.toMap.foreach((pair) => {
            payload.put(pair._1, pair._2)
          })
//          return Right(grok_result.toMap)
        }
      }
    }
//    logger.warn("grok failed: (" + message + ") try types: " + types + " " + ctx.all())
//    ctx.metric(MetricTypes.metric_pattern_fail)
//    Left(null)
    new IntermediateEvent(payload)
  }

//  override def handle_preview(ctx: PreviewContext): Either[Throwable, Iterable[(String,(Int, Int, String))]] = {
//    val types   =  {
//     matches.size match {
//       case 0 => Seq(ctx.get("type",""))
//       case _ => matches
//      }
//    }
//
//    val (start, end, message) = ctx.get_preview(cfg.target, (-1, -1, ""))
//
//    //先试type, 再试matcher列表
//    for (typ <- types) {
//      val pattern_opt = compiled_patterns.get(typ)
//      if (pattern_opt.isEmpty) {
//        ctx.metric(MetricTypes.metric_no_pattern)
//      }
//      else {
//
//        val (names, pattern) = pattern_opt.get
//
//        val (grok_parse_ok, grok_result) = match_pattern_with_position(names, pattern, message)
//
//        if (grok_parse_ok) {
//          return Right (
//            grok_result.map { case (name, (start_pos, end_pos, value)) =>
//              if (start_pos == -1 || end_pos == -1)
//                (name, (start, end, value))
//              else
//                (name, (start_pos, end_pos, value))
//            }
//          )
//        }
//      }
//    }
//    logger.warn("grok failed: (" + message + ") try types: " + types + " " + ctx.all())
//    ctx.metric(MetricTypes.metric_pattern_fail)
//    Left(null)
//  }
}
