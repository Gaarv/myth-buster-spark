package com.octo.mythbuster.spark.sql.catalyst.parser

import com.octo.mythbuster.spark.sql.catalyst.lexer

import scala.util.parsing.combinator.Parsers
import scala.util.parsing.input.{ NoPosition, Position, Reader }

// https://github.com/enear/parser-combinators-tutorial/blob/master/src/main/scala/co/enear/parsercombinators/lexer/WorkflowLexer.scala

object Parser extends Parsers {

  override type Elem = lexer.Token

  class TokenReader(tokens: Seq[lexer.Token]) extends Reader[lexer.Token] {

    override def first: lexer.Token = tokens.head

    override def atEnd: Boolean = tokens.isEmpty

    override def pos: Position = tokens.headOption.map(_.pos).getOrElse(NoPosition)

    override def rest: Reader[lexer.Token] = new TokenReader(tokens.tail)

  }

  def table = { identifier } ^^ { case lexer.Identifier(tableName) => Table(tableName) }

  def relations = { table ~ rep(lexer.Comma() ~ table ) } ^^ { case firstTableRelation ~ otherTableRelations => firstTableRelation :: otherTableRelations.map({ case lexer.Comma() ~ tableRelation => tableRelation }) }

  def tableColumn = { identifier ~ lexer.Dot() ~ identifier } ^^ { case lexer.Identifier(tableName) ~ _ ~ lexer.Identifier(columnName) => TableColumn(tableName, columnName) }

  def projections = { tableColumn ~ rep(lexer.Comma() ~ tableColumn) } ^^ { case head ~ tail => (head :: tail.map({ case _ ~ columnExpression => columnExpression })).toSeq }

  def identifier = { accept("identifier", { case identifier @ lexer.Identifier(_) => identifier }) }

  def equal = { tableColumn ~ lexer.Equal() ~ tableColumn } ^^ { case leftColumnExpression ~ _ ~ rightColumnExpression => Equal(leftColumnExpression, rightColumnExpression) }

  def filter = equal

  def select: Parser[Select] = { lexer.Select() ~ projections ~ lexer.From() ~ relations ~ lexer.Where() ~ filter } ^^ { case _ ~ projections ~ _ ~ relations ~ _ ~ filter => Select(projections, filter, relations) }

  def ast = phrase(select)

  def apply(tokens: Seq[lexer.Token]): AST = {
    val reader = new TokenReader(tokens)
    ast(reader) match {
      case NoSuccess(message, next) => null
      case Success(ast, next) => ast
    }
  }

}