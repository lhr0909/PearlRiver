//package com.hoolix.pipeline.filter
//
//import com.hoolix.pipeline.core.{Context, Filter, FilterConfig, MetricTypes}
//import com.jayway.jsonpath.internal.path.PathCompiler
//import com.jayway.jsonpath.{DocumentContext, InvalidJsonException, JsonPath, PathNotFoundException}
//import net.minidev.json.{JSONArray, JSONObject}
//import org.apache.commons.lang3.StringUtils
//
//import scala.annotation.tailrec
//import scala.collection.mutable
//import scala.collection.JavaConversions._
//
///**
//  * json filter by json path
//  *
//  * @param paths field's jsonPath value
//  *
//  * @see https://github.com/jayway/JsonPath
//  */
//
//case class JsonFilter(cfg:FilterConfig, paths: Seq[String], max_level:Int = 3) extends  Filter{
//
//  /**
//    * gererate field name from json path
//    */
//  def Jsonpath2Fieldname(path_orig:String) : Seq[String] = {
//
//    var path = PathCompiler.compile(path_orig).toString
//    val names = mutable.Buffer[String]()
//
//    val token_pattern = "^\\['?(.*?)'?\\]".r
//    val func_pattern  = "^\\.(\\w+)\\(.*\\)".r
//
//    while (path != "") {
//      path match {
//        //skip space
//        case s if s.startsWith("\\s") => { path = path.drop(1) }
//        //skip $
//        case s if s.startsWith("$") => { path = path.drop(1) }
//        //replace .. to all
//        //TODO replace .. with key or index
//        case s if (path.startsWith("..")) => { path = path.drop(2)}
//        //replace function .length() to length
//        case s if (path.startsWith(".")) => {
//          val func_opt = func_pattern.findFirstIn(path)
//          if (func_opt.isEmpty) {
//            logger.error("unsupported function grammar: "+s)
//            path = path.drop(s.size)
//          }
//          else {
//
//            val func = func_opt.get
//            func match {
//              case func_pattern(name) => {
//                names.append(name)
//              }
//            }
//            path = path.drop(func.size)
//          }
//
//        }
//        //replace token to name
//        case s if (path.startsWith("[")) => {
//          val token_opt = token_pattern.findFirstIn(path)
//          //TODO
//          val token = token_opt.get
//          token match {
//            case token_pattern(name) => {
//              name match {
//                case "*" =>
//                case "?" =>
//                case s if s.contains(":") =>
//                case s if s.contains(",") =>
//                case s => names.append(s)
//              }
//              path = path.drop(token.size)
//            }
//          }
//        }
//        case s if s.startsWith("\\w\\+") => {
//          val word_pat = "(\\w\\+)".r
//          s match {
//            case word_pat(word) =>
//              names.append(word)
//              path = path.drop(word.length)
//          }
//        }
//        case s => {
//          println("unsupported grammar: "+s)
//          path=path.drop(s.size)
//        }
//      }
//    }
//
//    return names
//  }
//
//  lazy val paths_with_name = paths.map { case path =>
//    (path, Jsonpath2Fieldname(path))
//  }
//
//  private def putJsonValueRecursively(prefix:String, names:Seq[String], obj:Any, pool:mutable.HashMap[String,String], lv:Int) :Unit = {
//    if (lv >= max_level) {
//      pool.put(prefix + names.mkString("-"), obj.toString)
//      logger.warn("json filter max level reached: "+prefix + " " + names)
//      return
//    }
//    if (obj == null) {
//      return
//    }
//    else if (obj.isInstanceOf[JSONArray]) {
//      obj.asInstanceOf[JSONArray].zipWithIndex.foreach { case (value, index) =>
//        putJsonValueRecursively(prefix, Seq(index.toString), value, pool, lv + 1)
//      }
//    }
//    //map
//    else if (obj.isInstanceOf[JSONObject]) {
//      obj.asInstanceOf[JSONObject].foreach { case (key, value) =>
//        putJsonValueRecursively(prefix, Seq(key), value, pool, lv + 1)
//      }
//    }
//    else if (obj.isInstanceOf[java.util.HashMap[String,Any]]) {
//      obj.asInstanceOf[java.util.HashMap[String,Any]].foreach { case (key, value) =>
//        putJsonValueRecursively(prefix, Seq(key), value, pool, lv + 1)
//      }
//    }
//    else {
//      pool.put(prefix+names.mkString("-"), obj.toString)
//    }
//  }
//
//  /**
//    * @param ctx
//    * @return
//    * true  parse success
//    * false parse fail
//    * None  nothing to do
//    */
//  override def handle(ctx: Context): Either[Throwable, Iterable[(String,Any)]] = {
//
//    var json:DocumentContext = null
//    val value = ctx.get(cfg.target) match {
//      case Some("") | Some(null) | None =>
//        return Left(null)
//      case Some(value) => value
//    }
//
//    try {
//      json = JsonPath.parse(value.trim)
//    } catch {
//      case e  =>
//        ctx.metric(MetricTypes.metric_json_parse_error)
//        logger.error(s"parse json field ${cfg.target} error:"+ctx.message.trim+";", e)
//        return Left(e)
//    }
//
//    val pool = mutable.HashMap[String,String]()
//    for ((path, name) <- paths_with_name) {
//      try {
//        val obj: Any = json.read(path);
//        putJsonValueRecursively("", name, obj, pool, 0)
//      } catch {
//        case e: PathNotFoundException =>
//          ctx.metric(MetricTypes.metric_json_path_not_found)
//        case e =>
//          logger.error(s"json unknown field name:$name; path:$path;", e)
//          ctx.metric(MetricTypes.metric_json_get_path_error)
//      }
//    }
//    Right(pool)
//  }
//
//  override def toString: String = {
//    super.toString + " " + paths_with_name.map(_._2)
//  }
//}
