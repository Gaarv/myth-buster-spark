package octo.sql.plan.physical

object PhysicalPlanOptimizer extends PlanOptimizer[PhysicalPlan] {

  override val rules: Seq[Rule[PhysicalPlan]] = CodeGenerationRule :: Nil

}
