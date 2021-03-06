package com.octo.mythbuster.spark.sql.parser

import com.octo.mythbuster.spark.sql.expression.BinaryOperation

import scala.{util => u}
import scala.util.parsing.combinator.Parsers
import scala.util.parsing.input.{NoPosition, Position, Reader}
import scala.language.postfixOps
import com.octo.mythbuster.spark.sql.{expression => e}
import com.octo.mythbuster.spark.sql.{lexer => l}

// https://en.wikipedia.org/wiki/LR_parser
// https://gist.github.com/kishida/1345875
// https://github.com/stephentu/scala-sql-parser/blob/master/src/main/scala/parser.scala
// https://github.com/enear/parser-combinators-tutorial/blob/master/src/main/scala/co/enear/parsercombinators/lexer/Workflowl.scala

object Parser extends Parsers {

  override type Elem = l.Token

  class TokenReader(tokens: Seq[l.Token]) extends Reader[l.Token] {

    override def first: l.Token = tokens.head

    override def atEnd: Boolean = tokens.isEmpty

    override def pos: Position = tokens.headOption.map(_.pos).getOrElse(NoPosition)

    override def rest: Reader[l.Token] = new TokenReader(tokens.tail)

  }

  def or: Parser[e.Expression] = and * ( l.Or() ^^^ { (left: e.Expression, right: e.Expression) => e.Or(left, right) } )

  def and: Parser[e.Expression] = comparator * ( l.And() ^^^ { (left: e.Expression, right: e.Expression) => e.And(left, right) } )


  def comparator: Parser[e.Expression] = primaryExpression ~ rep(( l.Less() | l.Greater() | l.Equal() ) ~ primaryExpression ) ^^ { case lhs ~ elements =>
    elements.foldLeft(lhs) {
      case (acc, l.Greater() ~ rhs) => e.Greater(acc, rhs)
      case (acc, l.Less() ~ rhs) => e.Less(acc, rhs)
      case (acc, l.Equal() ~ rhs) => e.Equal(acc, rhs)
    }

  }

  def primaryExpression: Parser[e.Expression] = { column | literal | ( l.LeftParenthesis() ~> expression <~ l.RightParenthesis() ) }

  def expression = or

  def column = { identifier } ^^ { case l.Identifier(columnName) => e.TableColumn(columnName) }

  def identifier = { accept("identifier", { case identifier @ l.Identifier(_) => identifier }) }

  def literal: Parser[e.Literal] = { number | text | bool }

  def text = { accept("text", { case text @ l.Text(value) => value }) } ^^ { value => e.Text(value) }

  def number = { accept("number", { case number @ l.Number(value) => value }) } ^^ { value => e.Number(value) }

  def bool = { ( l.True() ^^ { _ => e.Bool(true) } ) | ( l.False() ^^ { _ => e.Bool(false) } ) }

  def select = { l.Select() ~> projections ~ relations ~ ( where ? ) } ^^ { case projections ~ relations ~ filter => Select(projections, filter, relations) }

  def from = l.From() ~> relations

  def where = l.Where() ~> expression

  def projection = expression ~ opt(l.As() ~ identifier) ^^ {
    case expression ~ None => expression
    case expression ~ Some(_ ~ l.Identifier(value)) => e.Alias(expression, value)
  }

  def projections = repsep(projection, l.Comma())

  def relations: Parser[Seq[Relation]] = l.From() ~> rep1sep(relation, l.Comma())

  def subSelect: Parser[Relation] = { l.LeftParenthesis() ~ select ~ l.RightParenthesis() } ^^ { case _ ~ select ~ _ => select }

  def relation: Parser[Relation] = ( table | subSelect ) ~ rep(l.Join() ~ table ~ l.On() ~ equal ^^ { case _ ~ relation ~ _ ~ filter => (relation, filter) } ) ^^ {
    case lhs ~ elements => elements.foldLeft(lhs) { case (leftRelation, (rightRelation, filter)) => Join(filter, leftRelation, rightRelation) }
  }

  def table: Parser[Relation] = { identifier } ^^ { case l.Identifier(tableName) => Table(tableName) }

  def equal : Parser[BinaryOperation] = primaryExpression ~ l.Equal() ~ primaryExpression ^^ { case leftColumn ~ _ ~ rightColumn => e.Equal(leftColumn, rightColumn) }

  def join = { relation ~ l.Join() ~ table ~ l.On() ~ equal } ^^ { case leftRelation ~ _ ~ rightRelation ~ _ ~ filter => Join(filter, leftRelation, rightRelation) }

  def ast: Parser[AST] = select

  def apply(tokens: Seq[l.Token]): u.Try[AST] = {
    val reader = new TokenReader(tokens)
    ast(reader) match {
      case NoSuccess(message, _) => u.Failure(new ParserException(message))
      case Success(ast, _) => u.Success(ast)
    }
  }

}