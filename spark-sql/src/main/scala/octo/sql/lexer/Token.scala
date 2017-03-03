package octo.sql.lexer

import com.octo.mythbuster.spark.sql.Row

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

case class Join() extends Token

case class On() extends Token

case class Greater() extends Token

case class Less() extends Token

case class Or() extends Token

case class And() extends Token

case class LeftParenthesis() extends Token

case class RightParenthesis() extends Token

sealed trait Literal[A] extends Token {

  val value: A

}

case class Text(value: String) extends Literal[String]

case class Number(value: Float) extends Literal[Float]

case class True() extends Token

case class False() extends Token

