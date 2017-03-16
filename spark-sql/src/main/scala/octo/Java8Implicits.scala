package octo

import java.util.Optional

trait Java8Implicits {

  implicit class OptionalImplicits[T](optional: Optional[T]) {

    def asScala(): Option[T] = if (optional.isPresent) Some(optional.get) else None

  }

  implicit class OptionImplicits[T](option: Option[T]) {

    def asJava(): Optional[T] = option match {
      case Some(t) => Optional.of(t)
      case None => Optional.empty()
    }

  }

}
