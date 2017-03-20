package octo.sql.plan.logical

object LogicalPlanOptimizer extends PlanOptimizer[LogicalPlan] {

  override val rules: Seq[Rule[LogicalPlan]] = Seq()

}
