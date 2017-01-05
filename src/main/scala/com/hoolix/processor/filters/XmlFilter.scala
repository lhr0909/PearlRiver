//package com.hoolix.pipeline.filter
//
//import java.io.StringReader
//import javax.xml.parsers.DocumentBuilderFactory
//import javax.xml.xpath.{XPathConstants, XPathFactory}
//
//import com.hoolix.pipeline.core.{Context, Filter, FilterConfig, MetricTypes}
//import org.apache.commons.lang3.StringUtils
//import org.w3c.dom.{Document, Node, NodeList}
//import org.xml.sax.InputSource
//
//import scala.collection.JavaConversions._
//import scala.collection.mutable
//import scala.xml.XML
//
//case class XmlFilter(cfg:FilterConfig, paths: Seq[String], max_level:Int = 10) extends  Filter{
//
//  def mkXmlString(seq:Seq[String], delimiter:String): String = {
//    val builder = new StringBuilder()
//    seq.foreach { s =>
//      if (builder.size != 0 && ! s.startsWith("#")) {
//        builder.append(delimiter)
//      }
//      builder.append(s)
//    }
//    return builder.toString()
//  }
//
//  def putXmlRecursively(prefix:Seq[String], pool:mutable.HashMap[String,String], node: NodeList, lv:Int) :Unit = {
//    if (lv >= max_level) {
//      logger.warn("max level reached, break: "+prefix+" "+node)
//      return
//    }
//    if (node.getLength == 0) {
//      return
//    }
//
//    for (i <- 0 until node.getLength) {
//      val item = node.item(i)
//      var value = item.getNodeValue
//      if (value != null ) {
//        value = value.trim
//        if (value != "") {
//          item.getNodeType match {
//            //do not add #text attribute name
//            case Node.TEXT_NODE =>
//              pool.put(prefix.mkString("-"), value)
//            case _ =>
//              pool.put(mkXmlString(prefix ++ Seq(item.getNodeName), "-"), value)
//          }
//        }
//        //skip empty node
//      }
//
//      //attribute
//      val attributes = item.getAttributes
//      if (attributes != null) {
//        for (i <- 0 until attributes.getLength) {
//          val attr = attributes.item(i)
//          pool.put(prefix.mkString("-")+"#"+attr.getNodeName, attr.getNodeValue)
//        }
//      }
//      putXmlRecursively(prefix ++ Seq(item.getNodeName), pool, item.getChildNodes, lv + 1)
//    }
//  }
//  val prefix = Seq()
//
//  override def handle(ctx: Context): Either[Throwable, Iterable[(String,Any)]] = {
//
//    val message = ctx.get(cfg.target) match {
//      case Some("") |Some(null)| None => return Left(null)
//      case Some(value)     => value
//    }
//
//    var doc:Document = null
//    try {
//      val factory = DocumentBuilderFactory.newInstance()
//      val builder = factory.newDocumentBuilder()
//      doc = builder.parse(new InputSource(new StringReader(message.trim)))
//    }catch {
//      case e =>
//        logger.error(s"parse xml field ${cfg.target} error:"+message.trim+";", e)
//        ctx.metric(MetricTypes.metric_xml_parse_error)
//        return Left(e)
//    }
//
//    val xPathfactory = XPathFactory.newInstance()
//    val xpath = xPathfactory.newXPath()
//
//    val pool = mutable.HashMap[String,String]()
//
//    paths.foreach { path =>
//      try {
//        val expr = xpath.compile(path)
//        val nodes = expr.evaluate(doc, XPathConstants.NODESET).asInstanceOf[NodeList]
//        putXmlRecursively(prefix, pool, nodes, 0)
//      } catch {
//        case e:javax.xml.transform.TransformerException =>
//          ctx.metric(MetricTypes.metric_xml_get_path_error)
//          logger.error("xpath compile error: "+path)
//      }
//    }
//    Right(pool)
//  }
//
//}
