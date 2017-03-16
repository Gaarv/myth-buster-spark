package octo

import scala.util.{ Try, Success, Failure }

object Implicits extends Java8Implicits {

  implicit class OptionToTry[A](option: Option[A]) {

    def toTry: Try[A] = toTry("Option is None")

    def toTry(m: String): Try[A] = toTry(new Exception(m))

    def toTry(e: Exception): Try[A] = option match {
      case Some(a) => Success(a)
      case None => Failure(e)
    }

  }

}
