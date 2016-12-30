package com.hoolix.processor

import akka.actor.{ActorSystem, Props}
import akka.routing.FromConfig
import com.hoolix.processor.actors.KafkaStreamFetcher
import com.typesafe.config.ConfigFactory


/**
  * Hoolix 2016
  * Created by simon on 12/29/16.
  */
object XYZProcessorMain extends App {
  override def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load
    val system = ActorSystem("xyz-processor", config)
    val fetchers = system.actorOf(FromConfig.props(Props[KafkaStreamFetcher]), "fetchers")


  }
}
