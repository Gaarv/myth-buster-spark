package octo.sql.plan.logical

import octo.sql.plan.Plan
import octo.{tree => t}
import octo.sql.{expression => e}
import octo.sql.{parser => p}

import scala.util.{Failure, Success, Try}

sealed trait LogicalPlan extends Plan[LogicalPlan]

case class CartesianProduct(leftChild: LogicalPlan, rightChild: LogicalPlan) extends LogicalPlan with t.BinaryTreeNode[LogicalPlan]

case class Filter(child: LogicalPlan, expression: e.Expression) extends LogicalPlan with t.UnaryTreeNode[LogicalPlan]

case class Projection(child: LogicalPlan, expression: Seq[e.Expression]) extends LogicalPlan with t.UnaryTreeNode[LogicalPlan]

case class Scan(tableName: String) extends LogicalPlan with t.LeafTreeNode[LogicalPlan]

object LogicalPlan {

  def apply(ast: Seq[p.AST]): Try[LogicalPlan] = ast match {
    case (relation: p.Relation) :: Nil => apply(relation)
    case (relation: p.Relation) :: otherRelations => for {
      lr <- apply(relation)
      rr <- apply(otherRelations)
    } yield CartesianProduct(lr, rr)
  }

  def apply(ast: p.AST): Try[LogicalPlan] = ast match {
    case p.Table(tableName) => Success(Scan(tableName))
    case p.Join(filter, leftRelation, rightRelation) => for {
      lr <- apply(leftRelation)
      rr <- apply(rightRelation)
    } yield Filter(CartesianProduct(lr, rr), filter)
    case p.Select(projections, Some(filter), relations) => for {
      r <- apply(relations)
    } yield Projection(Filter(r, filter), projections)
    case p.Select(projections, None, relations) => for {
      r <- apply(relations)
    } yield Projection(r, projections)
  }

}


