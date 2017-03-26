package com.octo.mythbuster.spark.sql.plan.physical

import com.octo.mythbuster.spark.sql.plan.{PlanOptimizer, Rule}
import com.octo.mythbuster.spark.sql.plan.physical.codegen.GenerateCode
import com.octo.mythbuster.spark.sql.plan.physical.rules._
import com.typesafe.config.Config

object PhysicalPlanOptimizer extends PlanOptimizer[PhysicalPlan] {

  private def generateCodeRule(implicit config: Config) = if (config.getBoolean("generateCode")) Seq(GenerateCode) else Seq()

  override def rules(implicit config: Config): Seq[Rule[PhysicalPlan]] = generateCodeRule ++ Nil

}
