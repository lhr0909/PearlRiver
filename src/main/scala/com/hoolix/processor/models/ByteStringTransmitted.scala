package com.hoolix.processor.models

/**
  * Hoolix 2017
  * Created by simon on 1/13/17.
  */
case class ByteStringTransmitted(offset: FileOffset, event: Event) extends ElasticSearchSinkable {
  override def indexName: String = event.indexName
  override def indexType: String = event.indexType
  override def docId: String = s"${ offset.fileName }.${ offset.offset }"
}
