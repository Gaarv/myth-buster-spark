package octo.sql.plan.logical

import octo.sql.plan.{PlanOptimizer, Rule}

object LogicalPlanOptimizer extends PlanOptimizer[LogicalPlan] {

  override val rules: Seq[Rule[LogicalPlan]] = Seq()

}
