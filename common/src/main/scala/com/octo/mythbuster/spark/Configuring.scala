package com.octo.mythbuster.spark

import com.typesafe.config.{ ConfigFactory }

trait Configuring {

  val config = ConfigFactory.load()

}
