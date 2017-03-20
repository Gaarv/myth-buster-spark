package com.octo.mythbuster.spark.sql.plan.logical

import com.octo.mythbuster.spark.sql.plan.{ PlanOptimizer, Rule }

object LogicalPlanOptimizer extends PlanOptimizer[LogicalPlan] {

  override val rules: Seq[Rule[LogicalPlan]] = Seq()

}
