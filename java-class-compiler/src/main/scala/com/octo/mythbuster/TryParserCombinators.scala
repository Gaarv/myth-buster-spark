package com.octo.mythbuster

trait Filter {

  def filter(text: String): Boolean

}

case class Contains(textPart: String) extends Filter {

  def filter(text: String): Boolean = text.contains(textPart)

}

class TryParserCombinators {

  def main(arguments: Array[String]): Unit = {
    // KEEP WHEN
  }

}
