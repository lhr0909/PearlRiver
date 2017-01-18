package com.hoolix.processor.models

import org.elasticsearch.action.bulk.{BulkRequest, BulkResponse}

/**
  * Hoolix 2017
  * Created by simon on 1/18/17.
  */
case class ElasticsearchBulkRequestContainer[SrcMeta <: SourceMetadata](
                                                                         id: Long,
                                                                         bulkRequest: BulkRequest,
                                                                         bulkResponse: Option[BulkResponse],
                                                                         bulkRequestError: Option[Exception],
                                                                         shippers: Seq[Shipper[SrcMeta, ElasticsearchPortFactory]]
                                                                       ) {}
