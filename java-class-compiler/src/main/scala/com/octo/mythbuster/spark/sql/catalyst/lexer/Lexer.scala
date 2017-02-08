package com.octo.mythbuster.spark.sql.catalyst.lexer

import scala.util.parsing.combinator.RegexParsers

object Lexer extends RegexParsers {

  override def skipWhitespace: Boolean = true

  def select: Parser[Select] = positioned { "SELECT" ^^ { _ => Select() } }

  def from: Parser[From] = positioned { "FROM" ^^ { _ => From() } }

  def where: Parser[Where] = positioned { "WHERE" ^^ { _ => Where() } }

  def dot: Parser[Dot] = positioned { "." ^^ { _ => Dot() } }

  def equal: Parser[Equal] = positioned { "=" ^^ { _ => Equal() } }

  def identifier: Parser[Identifier] = positioned { "[a-z][a-z0-9_]*".r ^^  { value => Identifier(value) } }

  def comma: Parser[Comma] = positioned { "," ^^ { _ => Comma() } }

  def tokens: Parser[Seq[Token]] = { phrase(rep1(select | from | where | dot | equal | comma | identifier)) } ^^ { _.toSeq }

  def apply(sql: String) = {
    parse(tokens, sql) match {
      case NoSuccess(messages, next) => {
        println(messages)
        Seq()
      }
      case Success(tokens, next) => tokens
    }
  }

}
