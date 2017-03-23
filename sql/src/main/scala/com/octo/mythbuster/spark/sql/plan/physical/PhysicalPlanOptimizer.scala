package com.octo.mythbuster.spark.sql.plan.physical

import com.octo.mythbuster.spark.sql.plan.{ PlanOptimizer, Rule }
import com.octo.mythbuster.spark.sql.plan.physical.codegen.GenerateCode
import com.octo.mythbuster.spark.sql.plan.physical.rules._

object PhysicalPlanOptimizer extends PlanOptimizer[PhysicalPlan] {

  override val rules: Seq[Rule[PhysicalPlan]] = Seq(GenerateCode)

}
