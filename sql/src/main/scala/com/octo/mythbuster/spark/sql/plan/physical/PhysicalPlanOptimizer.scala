package com.octo.mythbuster.spark.sql.plan.physical

import com.octo.mythbuster.spark.sql.plan.{PlanOptimizer, Rule}
import com.octo.mythbuster.spark.sql.plan.physical.codegen.GenerateJavaCode
import com.octo.mythbuster.spark.sql.plan.physical.rules._
import com.typesafe.config.Config

object PhysicalPlanOptimizer {

  def apply(config: Config) = new PhysicalPlanOptimizer(config)

}

// We're not using a object only, because we need to externalize the config creation in order to control the Java code generation
class PhysicalPlanOptimizer(val config: Config) extends PlanOptimizer[PhysicalPlan] {

  // The rule to generate Java code is enabled only if shouldGenerateCode=true
  private def generateJavaCodeRule = if (config.getBoolean("shouldGenerateCode")) Some(GenerateJavaCode) else None

  private def useHashJoinRule = if (config.getBoolean("shouldUseHashJoin")) Seq(UseHashJoin) else Nil

  override def rules: Seq[Rule[PhysicalPlan]] = useHashJoinRule

  override def optimizePlan(plan: PhysicalPlan): PhysicalPlan = generateJavaCodeRule match {
    case Some(generateJavaCodeRule) =>
      generateJavaCodeRule(super.optimizePlan(plan))
    case None =>
      super.optimizePlan(plan)
  }

}
