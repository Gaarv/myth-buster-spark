package com.octo.mythbuster.spark

import scala.util.{ Failure, Success, Try }

trait UtilImplicits {

  implicit class OptionUtilImplicits[Type](option: Option[Type]) {

    def toTry: Try[Type] = toTry("Option is None")

    def toTry(message: String): Try[Type] = toTry(new Exception(message))

    def toTry(exception: Exception): Try[Type] = option match {
      case Some(value) => Success(value)
      case None => Failure(exception)
    }

  }

}
