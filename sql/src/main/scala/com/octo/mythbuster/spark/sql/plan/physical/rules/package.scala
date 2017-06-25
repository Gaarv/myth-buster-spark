package com.octo.mythbuster.spark.sql.plan.physical

import com.octo.mythbuster.spark.sql.plan.Rule
import com.octo.mythbuster.spark.sql.{expression => e}
import com.octo.mythbuster.spark.{Logging, tree => t}

package object rules {

  object CollapseFilters extends Rule[PhysicalPlan] with Logging {

    override def apply(physicalPlan: PhysicalPlan): PhysicalPlan = physicalPlan.transformDown {
      case Filter(Filter(child, firstExpression), secondExpression) => Filter(child, e.And(firstExpression, secondExpression))
    }

  }

  // When the filter is Equal, then it replace the NestedLoopJoin by a HashJoin (but it may fail has we do not check the presence of the columns)
  object UseHashJoin extends Rule[PhysicalPlan] with Logging {

    override def apply(physicalPlan: PhysicalPlan): PhysicalPlan = physicalPlan.transformDown {
      case NestedLoopJoin(leftChild, rightChild, filter: e.Equal) =>
        logger.debug(s"The NestedLoopJoin between ${leftChild} and ${rightChild} on ${filter} can be replaced by an HashJoin")
        HashJoin(leftChild, rightChild, filter)
    }

  }

}
