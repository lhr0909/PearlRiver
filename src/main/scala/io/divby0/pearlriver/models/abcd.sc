val json =
  """
    |{"@timestamp":"2017-01-06T11:29:27.871Z","beat":{"hostname":"MacBook-Pro-2.lan","name":"MacBook-Pro-2.lan","version":"5.1.1"},"fields":{"token":"e5b654a4-b18c-4350-8564-fbebbaffc485"},"input_type":"log","message":"111.196.83.159 - - [06/Jul/2015:09:01:36 +0800] \"GET / HTTP/1.1\" 200 21582 \"-\" \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.124 Safari/537.36\"","offset":203,"source":"/Users/peiyuchao/Downloads/samplelog/apache_access.log","tags":["test"],"type":"test"}
  """.stripMargin

println(json)

import org.json4s._
import org.json4s.jackson.JsonMethods._

parse(json)


parse(json).transformField({
  case ("@timestamp", x) => ("timestamp", x)
  case ("input_type", x) => ("inputType", x)
  case ("type", x) => ("_type", x)
})

import io.divby0.pearlriver.models.FileBeatEvent

implicit val formats = DefaultFormats

parse(json).transformField({
  case ("@timestamp", x) => ("timestamp", x)
  case ("input_type", x) => ("inputType", x)
  case ("type", x) => ("_type", x)
}).extract[FileBeatEvent]