package com.octo.mythbuster.spark.sql.plan.logical

import com.octo.mythbuster.spark.sql.expression.{BinaryOperation, Expression}
import com.octo.mythbuster.spark.sql.plan.Plan
import com.octo.mythbuster.spark.{tree => t}
import com.octo.mythbuster.spark.sql.{ RelationName, expression => e, parser => p }

import scala.util.{Failure, Success, Try}

sealed trait LogicalPlan extends Plan[LogicalPlan]

case class CartesianProduct(leftChild: LogicalPlan, rightChild: LogicalPlan) extends LogicalPlan with t.BinaryTreeNode[LogicalPlan]

case class Join(leftChild: LogicalPlan, rightChild: LogicalPlan, operation : BinaryOperation) extends LogicalPlan with t.BinaryTreeNode[LogicalPlan]

case class Filter(child: LogicalPlan, expression: e.Expression) extends LogicalPlan with t.UnaryTreeNode[LogicalPlan]

case class Projection(child: LogicalPlan, expression: Seq[e.Expression]) extends LogicalPlan with t.UnaryTreeNode[LogicalPlan]

case class TableScan(tableName: TableName, aliasName: Option[RelationName]) extends LogicalPlan with t.LeafTreeNode[LogicalPlan]

object LogicalPlan {

  def apply(ast: Seq[p.AST]): Try[LogicalPlan] = ast match {
    case (relation: p.Relation) :: Nil => apply(relation)
    case (relation: p.Relation) :: otherRelations => for {
      lr <- apply(relation)
      rr <- apply(otherRelations)
    } yield CartesianProduct(lr, rr)
  }

  def apply(ast: p.AST): Try[LogicalPlan] = ast match {
    case p.Alias(p.Table(tableName), aliasName) => Success(TableScan(tableName, Some(aliasName)))
    case p.Table(tableName) => Success(TableScan(tableName, None))
    case p.Join(filter, leftRelation, rightRelation) => for {
      lr <- apply(leftRelation)
      rr <- apply(rightRelation)
    } yield Join(lr, rr, filter) //Filter(CartesianProduct(lr, rr), filter)
    case p.Select(projections, Some(filter), relations) => for {
      r <- apply(relations)
    } yield Projection(Filter(r, filter), projections)
    case p.Select(projections, None, relations) => for {
      r <- apply(relations)
    } yield Projection(r, projections)
  }

}


