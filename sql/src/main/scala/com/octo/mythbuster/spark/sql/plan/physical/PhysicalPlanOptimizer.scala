package com.octo.mythbuster.spark.sql.plan.physical

import com.octo.mythbuster.spark.sql.plan.{ PlanOptimizer, Rule }
import com.octo.mythbuster.spark.sql.plan.physical.codegen.CodeGenerationRule

object PhysicalPlanOptimizer extends PlanOptimizer[PhysicalPlan] {

  override val rules: Seq[Rule[PhysicalPlan]] = CodeGenerationRule :: Nil

}
