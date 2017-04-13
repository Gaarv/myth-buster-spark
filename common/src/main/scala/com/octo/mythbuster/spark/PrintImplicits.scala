package com.octo.mythbuster.spark

trait PrintImplicits {

  implicit class AnyImplicits(a: Any) {

    def print() = {
      println(Printer.print(a))
    }

  }

}
