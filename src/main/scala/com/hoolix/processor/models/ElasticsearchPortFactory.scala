package com.hoolix.processor.models

import com.hoolix.processor.models.events.{Event, FileBeatEvent, IntermediateEvent, XYZBasicEvent}
import org.elasticsearch.action.index.IndexRequest

/**
  * Hoolix 2017
  * Created by simon on 1/14/17.
  */
case class ElasticsearchPortFactory() extends PortFactory {
  type T = IndexRequest

  //FIXME: 这里match有些臃肿 看看有没有办法把这几种event聚起来 有一个common的调用方法

  def generateIndexName(event: Event): String = {
    event match {
      case fb: FileBeatEvent =>
        s"_filebeat_.${fb._type}"
      case xyz: XYZBasicEvent =>
        s"${xyz.token}.${xyz._type}"
      case int: IntermediateEvent =>
        s"${int.token}.${int._type}"
      case _ => "_unknown_"
    }
  }

  def generateIndexType(event: Event): String = {
    event match {
      case fb: FileBeatEvent => fb._type
      case xyz: XYZBasicEvent => xyz._type
      case int: IntermediateEvent => int._type
      case _ => "_unknown_"
    }
  }

  override def generateRequest(sourceMetadata: SourceMetadata, event: Event): T = {
    val indexName: String = generateIndexName(event)
    val indexType: String = generateIndexType(event)
    val docId: String = sourceMetadata.id

    new IndexRequest(indexName, indexType, docId)
  }
}
