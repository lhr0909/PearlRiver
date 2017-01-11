package com.hoolix.processor.models

import org.elasticsearch.action.index.IndexRequest

/**
  * Created by peiyuchao on 2017/1/5.
  */
trait ElasticSearchSinkAble {
  def indexName: String
  def indexType: String
  def docId: String
  def toIndexRequest: IndexRequest = new IndexRequest(indexName, indexType, docId)
}
