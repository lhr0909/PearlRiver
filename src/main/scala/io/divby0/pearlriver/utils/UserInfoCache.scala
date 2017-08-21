//package io.divby0.processor.utils
//import java.util.UUID
//import java.util.concurrent.ConcurrentHashMap
//import java.util.concurrent.atomic.AtomicLong
//
//import com.hoolix.pipeline.core.{Config, Context, MetricTypes}
//import org.slf4j.LoggerFactory
//
//case class UserInfoCache(db:MysqlClient, login_timeout_sec:Int) {
//
//  lazy val logger = LoggerFactory.getLogger(this.getClass)
//
//  //TODO expire cache
//  var token_to_index_cache = new ConcurrentHashMap[String,String]()
//  var token_missing_cache = new ConcurrentHashMap[String,AtomicLong]()
//
//  def load_cache() = {
//    try {
//      db.simple_query(
//        "select * from index_alias",
//        Seq("user", "indexs")
//      ).map { case entry =>
//        entry.get("user").getOrElse("") -> entry.get("indexs").getOrElse("")
//      }.foreach { case (k, v) =>
//        token_to_index_cache.put(k, v)
//      }
//    }catch {
//      case e:Exception => logger.error("Loading index cache failed: ", e)
//    }
//  }
//
//  def load_cache(token:String) : Option[String] = {
//    try {
//      val entry = db.simple_query(
//        "select * from index_alias where user = '%s'".format(token),
//        Seq("user", "indexs")
//      ).map { case entry =>
//        entry.get("user").get -> entry.get("indexs").get
//      }.toMap
//
//      entry.get(token) match {
//        case Some(index) =>
//          token_to_index_cache.put(token, index)
//          return Some(index)
//        case None =>
//          token_missing_cache.put(token, new AtomicLong(System.currentTimeMillis()))
//          return None
//      }
//    }catch {
//      case e:Exception =>
//        logger.error("Loading index cache for [%s] failed: ".format(token), e)
//        token_missing_cache.put(token, new AtomicLong(System.currentTimeMillis()))
//        return None
//    }
//  }
//
//  def token_to_index(token:String) : Option[String] = {
//    token_to_index_cache.getOrDefault(token, null) match {
//      case null  => None
//      case index => Some(index)
//    }
//  }
//
//  def token_to_index(ctx:Context, token:String) : Option[String] = {
//    token_to_index_cache.getOrDefault(token, null) match {
//      case null  =>
//      case index => return Some(index)
//    }
//
//    var last_reload_counter = token_missing_cache.getOrDefault(token, null)
//    if (last_reload_counter == null) {
//      last_reload_counter = new AtomicLong(System.currentTimeMillis());
//      if (token_missing_cache.put(token, last_reload_counter) != null) {
//        ctx.metric(MetricTypes.metric_token_cache_wait_reload)
//        return None;
//      }
//    }
//
//    val last_timestamp = last_reload_counter.get()
//    val current_timestamp = System.currentTimeMillis()
//
//    if (last_timestamp + ctx.config.spark_xyz_database_cache_reload_delay_ms.toInt <= current_timestamp) {
//      last_reload_counter.compareAndSet(last_timestamp, current_timestamp) match {
//        case true  =>
//          ctx.metric(MetricTypes.metric_token_cache_reload)
//          return load_cache(token)
//        case false =>
//          ctx.metric(MetricTypes.metric_token_cache_wait_reload)
//      }
//    }
//
//    ctx.metric(MetricTypes.metric_token_cache_miss)
//
//    return None;
//  }
//
//  load_cache()
//}
