//package io.divby0.processor.utils
//import java.sql.PreparedStatement
//import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
//import java.util.concurrent.atomic.AtomicLong
//import java.util.concurrent.locks.ReentrantLock
//
//import com.google.common.cache.{CacheBuilder, CacheLoader, RemovalListener, RemovalNotification}
//import com.hoolix.pipeline.core.{Metric, MetricTypes}
//import com.hoolix.pipeline.metric.XYZMetric
//import org.slf4j.LoggerFactory
//
//import scala.collection.JavaConversions._
//
//case class UserUsageCache(db:MysqlClient,
//                          metric:Metric,
//                          usage_cache_size:Int        = 2000,
//                          usage_cache_expire_sec:Int  = 20
//                         ) {
//
//  lazy val logger = LoggerFactory.getLogger(this.getClass)
//
//  //metrics usage bytes/drop bytes
//  private val usage_bytes = new ConcurrentHashMap[String, (AtomicLong, AtomicLong)]()
//  private var last_usage_bytes = Map[String, (Long,Long)]()
//
//  //user to company cache
//  case class UserUsageEntry(version:Long, company_id:String, limits:Long, current_usage:AtomicLong)
//
//  private val user_usage_sql_by_user =
//    """
//    SELECT users.uuid user_id, company.uuid company_id, company.limits limits, company.current_usage
//    FROM
//    (SELECT * FROM user where is_deleted = 0 and stop = 0 and uuid = '%s') users
//      LEFT JOIN
//      (SELECT * FROM user where uuid = owner and is_deleted = 0 and stop = 0) owners
//      ON users.owner = owners.uuid
//    LEFT JOIN company company
//      ON company.owner = users.uuid
//    """
//  private def load_user_usage_info(sql:String) = {
//    logger.debug("execute sql sql")
//    try {
//      db.simple_query(sql, Seq("user_id", "company_id", "limits", "current_usage")).map { row =>
//        val user_id = row.getOrElse("user_id", "")
//        user_id -> UserUsageEntry(
//          user_cache_version.incrementAndGet(),
//          row.getOrElse("company_id", ""),
//          row.getOrElse("limits", "0") match {
//            case null =>
//              logger.warn(s"limits not found for user `$user_id`")
//              0L
//            case s    => s.toLong
//          },
//          new AtomicLong(row.getOrElse("current_usage", "0") match {
//            case null =>
//              logger.warn(s"current_usage not found for user `$user_id`")
//              0L
//            case s    => s.toLong
//          })
//        )
//      }.toMap
//    } catch {
//      case e:java.sql.SQLException =>
//        logger.error("load user cache ", e)
//        Map()
//    }
//  }
//  private def load_by_user(user:String) : Option[UserUsageEntry] = {
//    val cache = load_user_usage_info(user_usage_sql_by_user.format(user)).toSeq
//    cache.length match {
//      case 0 => None
//      case 1 => Some(cache(0)._2)
//      case _ =>
//        logger.warn("multi user found for "+user)
//        Some(cache(0)._2)
//    }
//  }
//
//  //update usage sql
//  private val update_sql_template              = "update %s set total_usage=last_insert_id(total_usage+?),current_usage=last_insert_id(current_usage+?),total_drop=last_insert_id(total_drop+?),current_drop=last_insert_id(current_drop+?) where uuid=?"
//  private val update_user_usage_sql      = update_sql_template.format("user")
//  private val update_company_usage_sql   = update_sql_template.format("company")
//
//
//  val user_cache_version = new AtomicLong()
//
//  private val user_usage_cache = CacheBuilder.newBuilder()
//    .maximumSize(usage_cache_size)
//    .expireAfterWrite(usage_cache_expire_sec, TimeUnit.SECONDS)
//    .build(new CacheLoader[String, Option[UserUsageEntry]] {
//    override def load(user: String): Option[UserUsageEntry] = {
//
//      metric.count(MetricTypes.metric_usage_db_read)
//      val entry_opt = load_by_user(user)
//      if (entry_opt.isEmpty) {
//        logger.warn("usage not found for " + user)
//        metric.count(MetricTypes.metric_usage_user_not_found)
//      }
//      entry_opt
//    }
//  })
//
//  private def user_to_company(user_id: String): Option[String] = {
//    user_usage_cache.get(user_id) match {
//      case Some(entry) => Some(entry.company_id)
//      case None        => None
//    }
//  }
//
//  def check_usage(token:String, bytes:Long) : Boolean = {
//    user_usage_cache.get(token) match {
//      case None        => true
//      case Some(entry) =>
//
//        val curr_usage = getOrCreate(token, usage_bytes)
//        val last_usage = last_usage_bytes.getOrElse(token, (0L,0L))._1
//
//        curr_usage._1.get() + bytes - last_usage + entry.current_usage.get() > entry.limits match {
//          case true =>
//            metric.count(MetricTypes.metric_usage_reach_drop)
//            curr_usage._2.addAndGet(bytes)
//            //TODO remove later
//            //logger.warn("user [%s] reached limits: local:%s remote:%s limits:%s".format(token, curr_usage._1.get()+bytes-last_usage, entry.current_usage, entry.limits))
//            false
//          case false =>
//            curr_usage._1.addAndGet(bytes)
//            true
//        }
//    }
//  }
//
//  private def getOrCreate(name:String, map:ConcurrentHashMap[String,(AtomicLong, AtomicLong)]) = {
//    map.getOrDefault(name, null) match {
//      case null =>
//        map.putIfAbsent(name, (new AtomicLong(), new AtomicLong()))
//        map.get(name)
//      case counter => counter
//    }
//  }
//
//  private def usage_snapshot(curr:ConcurrentHashMap[String,(AtomicLong,AtomicLong)]) = {
//    curr.map { case(name, (usage,drop)) =>
//        name -> (usage.get(), drop.get())
//    }.toMap
//  }
//
//  private def recent_usage(curr:Map[String,(Long,Long)], last:Map[String,(Long,Long)]) = {
//    curr.map { case(name, curr_usage) =>
//      val last_usage = last.getOrElse(name, (0L, 0L))
//      name -> (curr_usage._1 - last_usage._1, curr_usage._2 - last_usage._2)
//    }
//  }
//
//  def report(): Unit = {
//    UserUsageCache.synchronized {
//      val curr_usage_bytes = usage_snapshot(usage_bytes)
//      val usage = recent_usage(curr_usage_bytes, last_usage_bytes).filter(_._1 != "").filter(_._2 != (0L,0L))
//
//      if (usage.size == 0)
//        return
//
//      try {
//        val recent_entry_version = usage.map { case (user_id, _) => user_id -> {
//          user_usage_cache(user_id) match {
//            case Some(entry) => entry.version
//            case None => -1
//          }
//        }}
//
//        val report_success = report_db(usage)
//
//        if (report_success) {
//          last_usage_bytes = curr_usage_bytes
//
//          //update local cache
//          usage.foreach { case (user_id, (curr_usage, curr_drop)) =>
//            user_usage_cache.get(user_id) match {
//              case None =>
//              case Some(entry) =>
//                if (entry.version == recent_entry_version.getOrElse(user_id, -1))
//                  entry.current_usage.addAndGet(curr_usage)
//            }
//          }
//        }
//        else
//          logger.error("report failed")
//      } catch {
//        case e => logger.warn("report error:", e)
//      }
//    }
//  }
//
//  def ignore(callable : () => Any, label:String="") = {
//    try {
//      callable()
//    } catch {
//      case e :java.sql.SQLException =>
//        logger.error(label, e)
//    }
//  }
//
//  /**
//    * @param usage userid -> usage_recent
//    */
//  def report_db(usage: Map[String,(Long,Long)]): Boolean = {
//
//
//    val conn = db.getConnection() match {
//      case Some(conn) => conn
//      case None => {
//        metric.count(MetricTypes.metric_usage_db_fail)
//        return false
//      }
//    }
//
//    val valid_usage = usage.filter(_._1 != "").filter(_._2 != (0L,0L))
//    if (valid_usage.size == 0)
//      return false
//
//    val ps_user    = conn.prepareStatement(update_user_usage_sql)
//    val ps_company = conn.prepareStatement(update_company_usage_sql)
//
//    valid_usage.foreach { case (user_id, (usage, drop)) =>
//      metric.count(MetricTypes.metric_usage_db_write)
//      ps_user.setLong(1, usage)
//      ps_user.setLong(2, usage)
//      ps_user.setLong(3, drop)
//      ps_user.setLong(4, drop)
//      ps_user.setString(5, user_id)
//      ps_user.addBatch()
//
//      user_to_company(user_id) match {
//        case None =>
//        case Some(company_id) =>
//          ps_company.setLong(1, usage)
//          ps_company.setLong(2, usage)
//          ps_company.setLong(3, drop)
//          ps_company.setLong(4, drop)
//          ps_company.setString(5, company_id)
//          ps_company.addBatch()
//      }
//    }
//
//    metric.count(MetricTypes.metric_usage_db_write_batch)
//    ignore(ps_user.executeUpdate, "ps_user")
//    ignore(ps_company.executeUpdate, "ps_company")
//    ignore(() => ps_user.closeOnCompletion())
//    ignore(() => ps_company.closeOnCompletion())
//    true
//  }
//}
//object UserUsageCache extends App{
//  val database   = MysqlClient("com.mysql.jdbc.Driver", "jdbc:mysql://vbox/xyz?characterEncoding=UTF-8", "root", "123456", 60)
//  val metric     =  XYZMetric()
//  val cache      = UserUsageCache(database,metric)
//  cache.check_usage("e5b654a4-b18c-4350-8564-fbebbaffc485", 10)
//  cache.check_usage("e5b654a4-b18c-4350-8564-fbebbaffc481", 11)
//  //cache.load()
//  println(cache.user_usage_cache)
//  println(cache.usage_bytes)
//  cache.report()
//  cache.check_usage("e5b654a4-b18c-4350-8564-fbebbaffc481", 12)
//  cache.report()
//  cache.check_usage("e5b654a4-b18c-4350-8564-fbebbaffc481", 13)
//  cache.check_usage("e5b654a4-b18c-4350-8564-fbebbaffc481", 13)
//  cache.check_usage("e5b654a4-b18c-4350-8564-fbebbaffc481", 13)
//  Thread.sleep(2000)
//  println()
//  cache.check_usage("e5b654a4-b18c-4350-8564-fbebbaffc481", 10)
//  cache.check_usage("e5b654a4-b18c-4350-8564-fbebbaffc485", 20)
//  cache.report()
//  println()
//  Thread.sleep(2000)
//  cache.check_usage("e5b654a4-b18c-4350-8564-fbebbaffc485", 20)
//  cache.report()
//  metric.dump()
//}
