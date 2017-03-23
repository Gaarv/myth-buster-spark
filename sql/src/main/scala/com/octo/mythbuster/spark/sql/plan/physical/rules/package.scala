package com.octo.mythbuster.spark.sql.plan.physical

import com.octo.mythbuster.spark.sql.plan.Rule
import com.octo.mythbuster.spark.sql.{ expression => e }
import com.octo.mythbuster.spark.{ tree => t }

package object rules {

  object CollapseFilters extends Rule[PhysicalPlan] {

    override def apply(physicalPlan: PhysicalPlan): PhysicalPlan = {
      physicalPlan.transformDown({
        case Filter(Filter(child, firstExpression), secondExpression) => Filter(child, e.And(firstExpression, secondExpression))
      })
    }

  }

}
