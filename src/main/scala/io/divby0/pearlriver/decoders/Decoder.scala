package io.divby0.pearlriver.decoders

import io.divby0.pearlriver.models.Event

trait Decoder {
  def decode(event: Event): Event
}

//object Decoder {
//   def getDecoder(name:String) : Decoder = {
//    name match {
//      case "json"     => JsonDecoder()
//      case "xyz-json" => JsonDecoder()
//      case "xyz-line" => XYZLineDecoder()
//      case "raw-line" => RawLineDecoder()
//      case "filebeat" => FileBeatDecoder()
//    }
//  }
//}
