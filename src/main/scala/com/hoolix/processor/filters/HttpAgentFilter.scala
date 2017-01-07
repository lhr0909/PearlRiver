package com.hoolix.processor.filters

import java.util.regex.Pattern

import com.hoolix.processor.models.{Event, IntermediateEvent}
import eu.bitwalker.useragentutils.UserAgent

case class HttpAgentFilter(targetField: String) extends Filter{
  /**
    * @param ctx
    * @return
    * true  parse success
    * false parse fail
    * None  nothing to do
    */
  override def handle(event: Event): Event = {
    val payload = event.toPayload


    val agent_str = Some(payload.get(targetField).asInstanceOf[Some[String]].get) match {
      case Some("")  => ""
      case Some("-") => ""
//      case None      => ""
      case Some(s)   => s
    }

//    if (agent_str == "" || agent_str == "-")
//      return Left(null)

    val (browser, os) = parse(agent_str)

//    Right(
//      Seq(
//        ("agent_browser", browser),
//        ("agent_os", os)
//      )
//    )
    payload.put("agent_browser", browser)
    payload.put("agent_os", os)
//    println("in agent filter")
//    println(payload)
    IntermediateEvent(payload)
  }

  def browser_tag = Seq(
    ("Maxthon"  -> "MAXTHON"), //遨游
    ("The world" -> "The world"), //世界之窗
    ("QQBrowser"-> "QQ"),
    ("QQDownload" -> "QQ"),
    ("GreenBrowser"->"Green"),
    ("360SE"    -> "360"),
    ("Opera"    -> "Opera"),
    ("Chrome"   -> "Chrome"),
    //("Safari"   -> "Safari"),
    ("SE "   -> "Sougou"),
    ("MetaSr"-> "Sougou"),
    ("SogouMobileBrowser" -> "Sougou Mobile"),
    ("UCWEB" -> "UC"),
    ("UCBrowser" -> "UC"),
    ("UBrowser" -> "UC"),
    ("TencentTraveler" -> "QQ Mobile"),
    ("MQQBrowser" -> "QQ Mobile"),
    ("TaoBrowser" -> "Taobao"),
    ("LBBROWSER" -> "Liebao"),
    ("HUAWEI"   -> "Huawei"),
    ("Huawei"   -> "Huawei"),
    ("Firefox"  -> "Firefox"),
    ("2345Explorer" -> "2345Explorer"),
    ("MicroMessenger" ->"Weixin")
    //("MSIE 9.0" -> "IE9"),
    //("MSIE 8.0" -> "IE8"),
    //("MSIE 7.0" -> "IE7"),
    //("MSIE 6.0" -> "IE6")
  )

  lazy val bot_agent_pattern = Pattern.compile("[\\w-]*[bB]ot[\\w-]*")
  lazy val spider_agent_pattern = Pattern.compile("[\\w-]*[sS]pider[\\w-]*")

  /**
    * 识别bot和spider
    */
  def parse_bot(agent_string: String): Option[String] = {
    if (agent_string.contains("bot")) {
      val matcher = bot_agent_pattern.matcher(agent_string)

      if(matcher.find())
        return Some(matcher.group(0))
    }
    else if (agent_string.toLowerCase.contains("spider")) {
      val matcher = spider_agent_pattern.matcher(agent_string)

      if(matcher.find())
        return Some(matcher.group(0))
    }
    return None
  }

  lazy val simple_agent_pattern = Pattern.compile("^([\\w-+]+)\\/[\\w-+]+")

  def force_parse(agent_string:String): String = {
    val matcher = simple_agent_pattern.matcher(agent_string)
    if (matcher.find()) {
      return matcher.group(1)
    }
    return "UNKNOWN"
  }

  def parse(agent_string: String) : (String,String) = {

    var browser_name = ""
    //国产浏览器
    browser_tag.find { case (tag,name) =>
       (agent_string.contains(tag)) match {
         case true  => browser_name = name; true
         case false => false
       }
    }

    val (browser, os) = {
      val agent = UserAgent.parseUserAgentString(agent_string)
      if (agent == "UNKNOWN") {
        ("", "")
      }
      else {
        agent.getBrowser.toString match {
          case "Unknown" => ("", agent.getOperatingSystem.toString)
          case "UNKNOWN" => ("", agent.getOperatingSystem.toString)
          case s if s.startsWith("IE")  => (s, agent.getOperatingSystem.toString)
          case s         => (s.toLowerCase.capitalize, agent.getOperatingSystem.toString)
        }
      }
    }

    if (browser_name == "") {
      browser_name = browser

      if (browser_name == "Bot")
        browser_name = parse_bot(agent_string).getOrElse("")
    }

    if (browser_name == "" || Seq("Unknown", "UNKNOWN").contains(browser_name)) {
      browser_name = force_parse(agent_string)
    }

    if (browser_name == "") {
      return ("UNKNOWN", os)
    } else {
      return (browser_name.capitalize, os)
    }
  }

}
