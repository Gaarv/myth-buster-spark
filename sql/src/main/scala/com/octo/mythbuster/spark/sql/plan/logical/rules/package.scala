package com.octo.mythbuster.spark.sql.plan.logical

import com.octo.mythbuster.spark.Logging
import com.octo.mythbuster.spark.sql.expression.And
import com.octo.mythbuster.spark.sql.plan.Rule

package object rules {

  object PushDownFilter extends Rule[LogicalPlan] with Logging {

    override val name = "Push Down Filter"

    override def apply(logicalPlan: LogicalPlan): LogicalPlan = logicalPlan transformDown {
      case Filter(Join(leftChild, rightChild, joinExpression), filterExpression) if filterExpression.consume.forall(leftChild.produce.contains(_)) =>
        Join(Filter(leftChild, filterExpression), rightChild, joinExpression)

      case Filter(Join(leftChild, rightChild, joinExpression), filterExpression) if filterExpression.consume.forall(rightChild.produce.contains(_)) =>
        Join(leftChild, Filter(rightChild, filterExpression), joinExpression)
    }

  }

  object CombineFilters extends Rule[LogicalPlan] with Logging {

    override val name = "Combine Filters"

    override def apply(logicalPlan: LogicalPlan): LogicalPlan = logicalPlan transformDown {
      case Filter(Filter(child, innerExpression), outerExpression) =>
        Filter(child, And(outerExpression, innerExpression))
    }

  }

  object SwitchProjectionAndFilter extends Rule[LogicalPlan] with Logging {

    override val name = "Switch Projection And Filter"

    override def apply(logicalPlan: LogicalPlan): LogicalPlan = logicalPlan transformDown {
      case Filter(Projection(child, expressions), predicate) =>
        Projection(Filter(child, predicate), expressions)
    }

  }

}
