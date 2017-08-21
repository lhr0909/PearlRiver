package io.divby0.pearlriver.modules

import java.net.InetAddress

import com.typesafe.config.Config
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

import scala.collection.JavaConversions

/**
  * Hoolix 2017
  * Created by simon on 1/4/17.
  */
object ElasticsearchClient {
  def apply()(implicit config: Config): TransportClient = {
    //TODO: add config validation checks
    val esConfig = config.getConfig("elasticsearch")

    val transportAddressesList = JavaConversions.asScalaBuffer(esConfig.getStringList("transport-addresses")).toList

    val esClient = new PreBuiltTransportClient(Settings.EMPTY)

    transportAddressesList map { item =>
      item.split(":").toList
    } foreach { item =>
      //TODO: error out when item doesn't have length of 2
      esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(item(0)), item(1).toInt))
    }

    esClient
  }

  def esIndexCreationSettings()(implicit config: Config): Settings.Builder = {
    val esConfig = config.getConfig("elasticsearch")

    val settingsBuilder = Settings.builder()

    JavaConversions.asScalaSet(esConfig.entrySet()) foreach { entry =>
      if (entry.getKey.startsWith("index")) {
        settingsBuilder.put(entry.getKey, esConfig.getString(entry.getKey))
      }
    }

    settingsBuilder
  }
}
