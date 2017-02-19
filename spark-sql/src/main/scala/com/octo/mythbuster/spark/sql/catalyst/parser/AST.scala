package com.octo.mythbuster.spark.sql.catalyst.parser

import com.octo.mythbuster.spark.sql.{ Row, TableName, ColumnName }
import com.octo.mythbuster.spark.sql.catalyst.expressions

/**
  * https://github.com/stephentu/scala-sql-parser/blob/master/src/main/scala/ast.scala
  */
sealed trait AST

trait Predicate extends expressions.Predicate with AST

trait Expression extends expressions.Expression with AST

case class TableColumn(tableName: TableName, columnName: ColumnName) extends Expression {

  override type Type = Any

  override def evaluate(row: Row): Type = row((tableName, columnName))

  override def toString() = tableName + "." + columnName

}

trait BinaryOperation extends Predicate {

  val leftChild: Expression

  val rightChild: Expression

}

case class Equal(leftChild: Expression, rightChild: Expression) extends BinaryOperation {

  override def evaluate(row: Row) = leftChild.evaluate(row) == rightChild.evaluate(row)

  override def toString() = leftChild + " == " + rightChild

}

trait Relation extends AST

case class Table(tableName: String) extends Relation

case class Select(projections: Seq[TableColumn], filter: Predicate, relations: Seq[Relation]) extends AST
