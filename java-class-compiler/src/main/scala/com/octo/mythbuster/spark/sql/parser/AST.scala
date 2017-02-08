package com.octo.mythbuster.spark.sql.parser

import com.octo.mythbuster.spark.{ Row, TableName, ColumnName }
import com.octo.mythbuster.spark.expression

/**
  * https://github.com/stephentu/scala-sql-parser/blob/master/src/main/scala/ast.scala
  */
sealed trait AST

trait Predicate extends expression.Predicate with AST

trait Expression extends expression.Expression with AST

case class TableColumn(tableName: TableName, columnName: ColumnName) extends Expression {

  override type Type = Any

  override def evaluate(row: Row): Type = row((tableName, columnName))

}

trait BinaryOperation extends Predicate {

  val leftChild: Expression

  val rightChild: Expression

}

case class Equal(leftChild: Expression, rightChild: Expression) extends BinaryOperation {

  override def evaluate(row: Row) = leftChild.evaluate(row) == rightChild.evaluate(row)

}

trait Relation extends AST

case class Table(tableName: String) extends Relation

case class Select(projections: Seq[Expression], filter: Predicate, relations: Seq[Relation]) extends AST
