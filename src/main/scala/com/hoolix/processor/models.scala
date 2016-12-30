package com.hoolix.processor

/**
  * Hoolix 2016
  * Created by simon on 12/29/16.
  */
package object models {
  case object Greet
  case class WhoToGreet(who: String)
  case class Greeting(message: String)
}
