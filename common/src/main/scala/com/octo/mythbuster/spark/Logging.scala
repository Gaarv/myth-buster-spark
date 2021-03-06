package com.octo.mythbuster.spark

import org.slf4j.{ Logger, LoggerFactory }

trait Logging {

  val loggerName: Option[String] = None

  val logger: Logger = LoggerFactory.getLogger(loggerName.getOrElse(getClass.getName))

}
