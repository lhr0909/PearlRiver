package io.divby0.pearlriver.filters.loaders

object ConfigEntryCheckErrorCode extends Enumeration {
  type ConfigEntryCheckErrorCode = Value

  val FieldRequired, Argument = Value

}


case class ConfigEntryCheckFailException
(
  config_type : String,
  config_name : String,
  //error_type  : ConfigEntryCheckErrorCode,
  message     : String
) extends Exception(message){


}

case class ConfigCheckFailException(msg: String) extends Exception(msg) {
}
