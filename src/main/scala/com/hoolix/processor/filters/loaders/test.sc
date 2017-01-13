import java.io.File

import com.hoolix.processor.models.User
import com.typesafe.config.ConfigFactory
import slick.driver.MySQLDriver.api._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Success, Try}
//import com.hoolix.processor.filters.loaders.ConfigLoader
//
//val config1 = ConfigLoader.load_from_yaml("/Users/peiyuchao/HuLiKeJi/XYZ-Processor/conf/pipeline/apache_access.yml")
//println(config1)
//
//val config = ConfigLoader.load_from_json("/Users/peiyuchao/HuLiKeJi/XYZ-Processor/conf/pipelinejson/apache_access.json")
//println(config)

//val configs = ConfigLoader.load_from_yaml(pipeline.conf.spark_xyz_conf_file_pipeline)
//PipelineBuilder.build_filter(configs, pipeline.conf)




val config = ConfigFactory.parseFile(new File("/Users/peiyuchao/HuLiKeJi/XYZ-Processor/conf/application.conf"))
val db = Database.forConfig("", config = config.getConfig("dev"))
try {
  val user = TableQuery[User]
  val q = user.filter(!_.isDeleted).map(_.uuid)
  val result = db.run(q.result)
  result.onComplete {
    case Success(res) => println(res)
    case (e) =>
  }
} finally db.close
//val db = Database.forConfig("")