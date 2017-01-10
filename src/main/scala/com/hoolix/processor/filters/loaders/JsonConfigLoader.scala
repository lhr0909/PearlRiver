package com.hoolix.processor.filters.loaders

/**
  * Created by peiyuchao on 2017/1/9.
  */
//class JsonConfigLoader {
//
//}


//  def load_from_json(json_str:String) = {
//    val root = JsonMethods.parse(json_str)
//
//    println(json_str)
//    val configs = root.children
//      .map { case node => {
//        val jsonWriter = new ObjectMapper()
//
//        val config_type = (node \ "type").extractOrElse("")
//        if (StringUtils.isBlank(config_type))
//          throw new ConfigCheckFailException("config_type is required to parse")
//
//        try {
//          RawConfigEntry(
//            token = (node \ "user_id").extractOrElse("*"),
//            name = (node \ "name").extractOrElse(""),
//            pool = (node \ "source_option" \ "pool").extractOrElse(null),
//            config_type = (node \ "type").extractOrElse(""),
//            record_type = (node \ "message_type").extractOrElse(""),
//            types = Seq((node \ "message_type").extractOrElse("")),
//            target = (node \ "source_field").extractOrElse(""),
//            field = jsonWriter.writeValueAsString((node \ "target_option").extractOrElse(null)),
//            require = jsonWriter.writeValueAsString((node \ "requires").extractOrElse(null)),
//            args = Converter.java_any_to_scala(new Yaml().load((node \ "argument").extractOrElse("")))
//          )
//        } catch {
//          case e =>
//            e.printStackTrace()
//            throw new ConfigCheckFailException(s"error while load config [$config_type]")
//        }
//      }
//      }
//    parse_raw_config_list(configs)
//  }