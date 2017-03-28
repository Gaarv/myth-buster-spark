package com.octo.mythbuster.spark.sql.plan.physical

import com.octo.mythbuster.spark.sql.plan.{PlanOptimizer, Rule}
import com.octo.mythbuster.spark.sql.plan.physical.codegen.GenerateJavaCode
import com.octo.mythbuster.spark.sql.plan.physical.rules._
import com.typesafe.config.Config

object PhysicalPlanOptimizer {

  def apply(config: Config) = new PhysicalPlanOptimizer(config)

}

class PhysicalPlanOptimizer(val config: Config) extends PlanOptimizer[PhysicalPlan] {

  private def generateCodeRule(shouldGeneratedCode: Boolean) = if (shouldGeneratedCode) Seq(GenerateJavaCode) else Nil

  override def rules: Seq[Rule[PhysicalPlan]] = generateCodeRule(config.getBoolean("shouldGenerateCode")) ++ Nil

}
