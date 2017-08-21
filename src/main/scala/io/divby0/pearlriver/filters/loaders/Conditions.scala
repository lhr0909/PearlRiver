package io.divby0.pearlriver.filters.loaders

import java.util.regex.{Matcher, Pattern}
import io.divby0.pearlriver.filters.loaders.ConditionOp.ConditionOp
import org.slf4j.LoggerFactory

object ConditionOp extends Enumeration {
  type ConditionOp = Value
  val True, False, NotExist, Exist, NotEqual, Equal, NotMatch, Match, NotFind, Find, Math = Value
}

//简单的and操作
case class Requires(conditions: Seq[Condition]) {
}

case class Condition(op: ConditionOp, pool:String = "global", key:String="", value:String="") {
}


object ConditionsBuilder {
  lazy val logger = LoggerFactory.getLogger(this.getClass)

  val condition_patterns = Seq(
    //(ConditionOp.Equal   , "^([^\\.]+)=(.+)$"),
    (ConditionOp.Equal   , "^(\\w+?)\\.(\\S+)==(.+)$"),
    (ConditionOp.NotEqual, "^(\\w+?)\\.(\\S+)!=(.+)$"),
    (ConditionOp.Equal   , "^(\\w+?)\\.(\\S+)=(.+)$"),
    (ConditionOp.Match   , "^(\\w+?)\\.(\\S+) contain (.+)$"),
    (ConditionOp.Match   , "^(\\w+?)\\.(\\S+) match (.+)$"),
    (ConditionOp.NotMatch, "^(\\w+?)\\.(\\S+) not match (.+)$"),
    (ConditionOp.Find    , "^(\\w+?)\\.(\\S+) find (.+)$"),
    (ConditionOp.NotFind , "^(\\w+?)\\.(\\S+) not find (.+)$"),
    (ConditionOp.Math    , "^(\\w+?)\\.(\\S+)([><])(.+)$"),
    (ConditionOp.Exist   , "^(\\w+?)\\.(\\S+)$"),
    (ConditionOp.Exist   , "^(\\w+?)\\.(\\S+) exist$"),
    (ConditionOp.NotExist, "^(\\w+?)\\.(\\S+) not exist$")
  ).map{case (op, pattern) => (op, Pattern.compile(pattern))}

  def build_condition(input: String) : Condition = {

    if (input == null || input.trim == "")
      return null

    var operation:ConditionOp.ConditionOp = null;
    var result:Matcher = null

    val found = condition_patterns.find { case (op, pattern) => {
      val matcher = pattern.matcher(input)
      matcher.find() match {
        case true => {
          operation = op;
          result = matcher;
          true
        };
        case false => false
      }
    }}.isDefined

    if (!found) {
      logger.error("unknown condition string: "+input)
      return null
    }

    val groups = (1 to result.groupCount()).map(result.group(_))

    if (groups.length == 0)
      throw new IllegalArgumentException("no match");
    if (groups.length == 1)
      Condition(operation, groups(0), "")
    else if (groups.length == 2)
      Condition(operation, groups(0), groups(1))
    else
      Condition(operation, groups(0), groups(1), groups(2))
  }

}
