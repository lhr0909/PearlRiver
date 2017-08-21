package io.divby0.pearlriver.modules

import akka.actor.ActorSystem
import akka.kafka.ConsumerSettings
import com.typesafe.config.Config
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.collection.JavaConversions

/**
  * Hoolix 2017
  * Created by simon on 1/4/17.
  */
object KafkaConsumerSettings {
  def apply()(implicit system: ActorSystem, config: Config): ConsumerSettings[Array[Byte], String] = {
    //TODO: add config validation checks
    val kafkaConfig = config.getConfig("kafka")

    val bootstrapServers = JavaConversions.asScalaBuffer(kafkaConfig.getStringList("bootstrap-servers")).toList.mkString(",")
    val groupId = kafkaConfig.getString("consumer-group-id")

    val properties = kafkaConfig.getConfig("properties")

    val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer)
                            .withBootstrapServers(bootstrapServers)
                            .withGroupId(groupId)

    JavaConversions.asScalaSet(properties.entrySet()) foreach { entry =>
      consumerSettings.withProperty(entry.getKey, properties.getString(entry.getKey))
    }

    consumerSettings
  }
}
