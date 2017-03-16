package octo.sql.plan.physical

import octo.sql.plan.{ PlanOptimizer, Rule }
import octo.sql.plan.physical.codegen.CodeGenerationRule

object PhysicalPlanOptimizer extends PlanOptimizer[PhysicalPlan] {

  override val rules: Seq[Rule[PhysicalPlan]] = CodeGenerationRule :: Nil

}
