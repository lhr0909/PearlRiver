//package com.hoolix.processor.configuration
//
//case class PipelineConfig[T]
//(
//  /**
//    * user_id -> config for each user
//    */
//  each_user: Map[String,PipelineConfigEachUser[T]]
//)
//{
//  def get = each_user.get(_)
//}
//
//
//case class PipelineConfigEachUser[T]
//(
//  token:String,
//  /**
//    * message_type -> config for each type
//    */
//  each_type: Map[String,PipelineConfigEachType[T]]
//)
//{
//  def get = each_type.get(_)
//}
//
//
//case class PipelineConfigEachType[T]
//(
//  `type`:String,
//  version:Long,
//
//  /**
//    *  config for each type
//    */
//  config: Option[PipelineTypeConfig] = None,
//
//  /**
//    * filters or filter_configs for each type
//    */
//  entries: Seq[T]
//)
//
//
//case class PipelineTypeConfig
//(
//  var mapping_file:Option[String] = None
//)
//
////object PipelineConfig {
////
////  type FilterCondition = (Context) => Boolean
////
////  type FilterWithCondition = (Seq[FilterCondition], Filter)
////}
