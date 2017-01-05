package com.hoolix.processor.utils
import java.net.InetAddress

import com.hoolix.pipeline.core.{ContextLocal, Metric, MetricTypes}
import com.hoolix.pipeline.metric.AccumulatorMetric
import com.hoolix.pipeline.output.es.BulkProcessor
import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool, GenericObjectPoolConfig}
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}
import org.elasticsearch.action.bulk.{BackoffPolicy, BulkRequest, BulkResponse}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.unit.{ByteSizeUnit, ByteSizeValue, TimeValue}
import org.elasticsearch.index.engine.VersionConflictEngineException
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.slf4j.LoggerFactory

import scala.collection.mutable

case class ESPool(cluster:String, addrs:String, timeout_ms:Long) {

  var es_pool_max      = Runtime.getRuntime().availableProcessors() * 4
  var es_pool_min_idle = Runtime.getRuntime().availableProcessors() / 2 + 1
  var es_pool_max_idle = Runtime.getRuntime().availableProcessors()

  var bulkMaxSize = 1000
  var bulkBufferSize = 3000
  var bulkMaxByte = 10 //M
  var bulkFlushSecond = 3 //s
  var bulkConcurrent = 6

  val client_settings = mutable.HashMap[String,String]()

  lazy val logger = LoggerFactory.getLogger(this.getClass)


  lazy val bulk_processor_pool = {
    val conf = new GenericObjectPoolConfig()
    logger.info(s"init es pool max:$es_pool_max max_idle:$es_pool_max_idle min_idle:$es_pool_min_idle")
    conf.setMaxTotal(es_pool_max)
    conf.setMinIdle(es_pool_min_idle)
    conf.setMaxIdle(es_pool_max_idle)

    new GenericObjectPool(BulkProcessorPool(cluster, addrs), conf)
  }

  def borrowObject(timeout: Long) : BulkProcessor = {
    bulk_processor_pool.borrowObject(timeout)
  }
  def borrowObject(timeout: Long, max_tries:Int) : BulkProcessor = {

    var processor:BulkProcessor = null
    var try_times = 0

    processor = bulk_processor_pool.borrowObject(timeout)

    if (processor == null) {
      do {
        processor = bulk_processor_pool.borrowObject(timeout)
        try_times += 1
      } while (processor == null && try_times < max_tries)
    }

    return processor
  }
  def returnObject(obj: BulkProcessor) : Unit = {
    if (obj == null)
        return
    bulk_processor_pool.returnObject(obj)
  }

  case class BulkProcessorPool(cluster:String, addrs:String) extends BasePooledObjectFactory[BulkProcessor] {

    override def wrap(obj: BulkProcessor): PooledObject[BulkProcessor] = new DefaultPooledObject(obj)

    override def create(): BulkProcessor = {

      val client = new_client()

      val bulkProcessor = {
        BulkProcessor.builder( client,
          new BulkProcessor.Listener() {
            override def beforeBulk(executionId: Long, request: BulkRequest, processor:BulkProcessor): Unit = {
              processor.extra match {
                case null   => logger.warn("no metric available for "+Thread.currentThread().getId)
                case metric_obj =>
                  val metric = metric_obj.asInstanceOf[Metric]
                  metric.count(MetricTypes.metric_es_bulk)
              }
            }

            override def afterBulk(executionId: Long, request: BulkRequest, response: BulkResponse, processor:BulkProcessor): Unit = {
              if (response.hasFailures)
                logger.error("after bulk: " + response.buildFailureMessage())

              //val (success, failure) = response.getItems.map { resp => resp.isFailed }.partition(_ == false)
              var success  = 0L
              var ver_fail = 0L
              var reject   = 0L
              var failure  = 0L

              response.getItems.foreach(resp =>
                if (!resp.isFailed) {
                  success += 1
                }
                else {
                  resp.getFailure.getStatus match {
                    //version CONFLICT
                    case RestStatus.CONFLICT => ver_fail += 1
                    //reject
                    case RestStatus.CONTINUE => reject += 1
                    case _ =>
                      failure += 1
                      println(resp.getFailure.getClass, resp.getFailure.getStatus)
                  }
                }
              )

              processor.extra match {
                case null => logger.warn("no metric available")
                case metric_obj => {
                  val metric = metric_obj.asInstanceOf[Metric]
                  metric.count(MetricTypes.metric_es_success,  success)
                  if (ver_fail != 0) {
                    metric.count(MetricTypes.metric_es_ver_fail, ver_fail)
                  }
                  if (reject != 0) {
                    metric.count(MetricTypes.metric_es_reject, reject)
                  }
                  if (failure != 0) {
                    metric.count(MetricTypes.metric_es_fail, failure)
                  }
                }
              }
            }
            override def afterBulk(executionId: Long, request: BulkRequest, failure: Throwable, processor:BulkProcessor): Unit = {
              failure.printStackTrace()
              logger.error("fail to send data to es %d", executionId)

              processor.extra match {
                case null => logger.warn("no metric available")
                case metric_obj =>
                  val metric = metric_obj.asInstanceOf[Metric]
                  metric.count(MetricTypes.metric_es_fail, request.subRequests().size())
              }
            }
          })
          .setBulkActions(bulkMaxSize)
          .setBulkSize(new ByteSizeValue(bulkMaxByte, ByteSizeUnit.MB))
          .setFlushInterval(TimeValue.timeValueSeconds(bulkFlushSecond))
          .setConcurrentRequests(bulkConcurrent)
          .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(50), 8))
          .setBufferActions(bulkBufferSize)
          .build()
      }
      bulkProcessor
    }


    override def destroyObject(p: PooledObject[BulkProcessor]): Unit = {
      try {
        p.getObject.flush()
        try { p.getObject.close()           } catch { case e => e.printStackTrace()}
        try { p.getObject.getClient.close() } catch { case e => e.printStackTrace()}
      } catch {
        case e => logger.error("destroy es client", e)
      }
      super.destroyObject(p)
    }
  }

  def new_client() = {
    val setting_builder = Settings.builder().put("cluster.name", cluster)
    client_settings.foreach { case (name, value) =>
        setting_builder.put(name, value)
    }
    val settings = setting_builder.build()
    val client = new PreBuiltTransportClient(settings)
    addrs.split(",").foreach { addr =>
      val (host, port) = addr.contains(":") match {
        case true =>
          val words = addr.trim.split(":")
          (words(0), words(1).toInt)
        case false => (addr.trim, 9300)
      }
      client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port))
    }
    client
  }

  def close(): Unit = {
    bulk_processor_pool.clear()
    bulk_processor_pool.close()
  }
  //TODO move to utils
  def add_request_with_backoff(bulkProcessor: BulkProcessor, req: IndexRequest, bulkBufferBackofInit:Int) = {
      var success = bulkProcessor.add(req)
      if (!success) {
        logger.info("[%s] %s enter es bulk backoff ".format(Thread.currentThread().getId, System.currentTimeMillis()))
        var i = 0
        while (!success) {

          val time_to_sleep = bulkBufferBackofInit * Math.min((1 << i), 1024)

          Thread.sleep(time_to_sleep)

          success = bulkProcessor.add(req)

          i += 1
        }
        logger.info("[%s] %s exit es bulk backoff with %s".format(Thread.currentThread().getId, System.currentTimeMillis(), i))
      }
  }

}
