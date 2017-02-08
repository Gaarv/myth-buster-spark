package com.octo.mythbuster.spark.sql.lexer

import scala.util.parsing.input.Positional

sealed trait Token extends Positional {

}

case class Select() extends Token

case class From() extends Token

case class Where() extends Token

case class Dot() extends Token

case class Identifier(value: String) extends Token

case class Equal() extends Token

case class Comma() extends Token
