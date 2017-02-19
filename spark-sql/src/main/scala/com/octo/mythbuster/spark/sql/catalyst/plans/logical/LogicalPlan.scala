package com.octo.mythbuster.spark.sql.catalyst.plans.logical

import com.octo.mythbuster.spark.sql.catalyst.expressions.Predicate
import com.octo.mythbuster.spark.sql.catalyst.parser
import com.octo.mythbuster.spark.sql.catalyst.parser.TableColumn

sealed trait LogicalPlan

case class CartesianProduct(leftChild: LogicalPlan, rightChild: LogicalPlan) extends LogicalPlan

case class Filter(child: LogicalPlan, predicate: Predicate) extends LogicalPlan

case class Projection(child: LogicalPlan, tableColumns: Seq[TableColumn]) extends LogicalPlan

case class Scan(tableName: String) extends LogicalPlan

object LogicalPlan {

  def apply(syntaxTrees: Seq[parser.AST]): LogicalPlan = syntaxTrees match {
    case (table: parser.Table) :: Nil => apply(table)
    case (relation: parser.Relation) :: otherRelations => CartesianProduct(apply(relation), apply(otherRelations))
  }

  def apply(syntaxTree: parser.AST): LogicalPlan = syntaxTree match {
    case parser.Table(tableName) => Scan(tableName)
    case parser.Select(projections, filter, relations) => Projection(Filter(apply(relations), filter), projections)
  }

}


