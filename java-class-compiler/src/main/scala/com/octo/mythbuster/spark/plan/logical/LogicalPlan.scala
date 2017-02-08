package com.octo.mythbuster.spark.plan.logical

import com.octo.mythbuster.spark.expression.Predicate
import com.octo.mythbuster.spark.sql.parser

sealed trait LogicalPlan

case class CartesianProduct(leftChild: LogicalPlan, rightChild: LogicalPlan) extends LogicalPlan

case class Filter(child: LogicalPlan, predicate: Predicate) extends LogicalPlan

case class Scan(tableName: String) extends LogicalPlan

object LogicalPlan {

  def apply(syntaxTrees: Seq[parser.AST]): LogicalPlan = syntaxTrees match {
    case (table: parser.Table) :: Nil => apply(table)
    case (relation: parser.Relation) :: otherRelations => CartesianProduct(apply(relation), apply(otherRelations))
  }

  def apply(syntaxTree: parser.AST): LogicalPlan = syntaxTree match {
    case parser.Table(tableName) => Scan(tableName)
    case parser.Select(projections, filter, relations) => Filter(apply(relations), filter)
  }

}


