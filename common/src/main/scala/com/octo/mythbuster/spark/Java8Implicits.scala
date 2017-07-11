package com.octo.mythbuster.spark

import java.util.{ Optional => JavaOptional }
import java.util.function.{ Supplier => JavaSupplier }

trait Java8Implicits {


  implicit class OptionalJava8Implicits[T](optional: JavaOptional[T]) {

    def asScala(): Option[T] = if (optional.isPresent) Some(optional.get) else None

  }

  implicit class OptionJava8Implicits[T](option: Option[T]) {

    def asJava(): JavaOptional[T] = option match {
      case Some(t) => JavaOptional.of(t)
      case None => JavaOptional.empty()
    }

  }

  implicit class Function0Java8Implicits[T](f: () => T) {

    def asJava(): JavaSupplier[T] = {
      () => f()
    }

  }

}
