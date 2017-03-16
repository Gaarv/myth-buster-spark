package octo.sql.plan

trait PlanOptimizer[P <: Plan[P]] {

  val rules: Seq[Rule[P]]

  def optimizePlan(plan: P): P = rules.foldLeft(plan) { (plan, rule) =>
    rule(plan)
  }

  def apply(plan: P): P = optimizePlan(plan)

}
