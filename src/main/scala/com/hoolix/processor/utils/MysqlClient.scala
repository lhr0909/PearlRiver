package com.hoolix.processor.utils
import java.sql.{Connection, DriverManager}

import com.avaje.ebean.EbeanServerFactory
import com.avaje.ebean.config.ServerConfig
import org.slf4j.LoggerFactory

import scala.collection.mutable

case class MysqlClient(driver:String, url:String, username:String, password:String, timeout_sec:Int=1) extends AutoCloseable{

  lazy val logger = LoggerFactory.getLogger(this.getClass)
  private var connection: Option[Connection] = None;
  def getConnection() = {
    if (connection.isDefined && connection.get.isClosed) {
      try {
        connection.get.close()
      } catch {
        case e =>
      }
      connection = None
    }
    if (connection.isEmpty) {
      this.synchronized {
        if (connection.isEmpty) {
          connection = connect()
        }
      }
    }
    connection
  }

  try {
    val ebean = EbeanUtil.configure(driver, url, username, password)
  }catch {
    case e => logger.warn("can't init ebeal:"+e.getMessage)
  }


  def connect() : Option[Connection] = {
    try {
      logger.info("connect database to "+url + " " + username + " " + password)
      Class.forName("com.mysql.jdbc.Driver")
      DriverManager.setLoginTimeout(timeout_sec)
      Some(DriverManager.getConnection(url, username, password))
    }catch {
      case e : java.net.SocketTimeoutException => {
        logger.error("connect to database timeout ", e)
        None
      }
      case e : com.mysql.jdbc.exceptions.jdbc4.CommunicationsException => {
        logger.error("CommunicationsException ", e)
        None
      }
      case e:Exception => {
        logger.error("Exception ", e)
        None
      }
    }
  }

  def simple_query(sql:String, labels: Seq[String]) : Seq[Map[String,String]]  ={
    val connection = getConnection()
    if (connection.isEmpty) {
      logger.error("Error cannot connect to "+url)
      return Seq()
    }

    val statement = connection.get.prepareStatement(sql)
    val row = statement.executeQuery()

    val data_list = mutable.ListBuffer[Map[String,String]]()

    while (row.next()) {
      val data_map  = mutable.Map[String, String]()
      for (label <- labels) {
        try {
          data_map.put(label, row.getString(label))
        }catch {
          case e : java.sql.SQLException => logger.error("query "+sql, e)
        }
      }
      data_list.append(data_map.toMap)
    }
    data_list.toSeq
  }

  def execute(): Unit = {
    val conn_opt = getConnection()
    if (conn_opt.isEmpty) {
      logger.warn("Error cannot connect to "+url)
      return Seq()
    }
    val conn = conn_opt.get
    conn.setAutoCommit(false)
    conn
  }

  override def close(): Unit = {
    try {
      if (connection.isDefined)
        connection.get.close()
    }
    catch {
      case e =>
    }

  }
}
