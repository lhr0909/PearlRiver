package com.hoolix.processor

import akka.actor.{ActorSystem}
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.Consumer
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.concurrent.Future


/**
  * Hoolix 2016
  * Created by simon on 12/29/16.
  */
object XYZProcessorMain extends App {
  override def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load
    implicit val system = ActorSystem("xyz-processor", config)
    implicit val materializer = ActorMaterializer()
    val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer)
      .withBootstrapServers("0.0.0.0:9092")
      .withGroupId("xyz-processor")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    val done = Consumer.committableSource(consumerSettings, Subscriptions.topics("hooli_topic"))
        .mapAsync(1) { msg =>
          println(msg.record.value)
          Future.successful(msg)
        }
        .mapAsync(1) { msg =>
          msg.committableOffset.commitScaladsl()
        }
        .runWith(Sink.ignore)
  }
}
