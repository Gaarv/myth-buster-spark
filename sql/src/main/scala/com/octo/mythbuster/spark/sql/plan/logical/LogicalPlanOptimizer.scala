package com.octo.mythbuster.spark.sql.plan.logical

import com.octo.mythbuster.spark.sql.plan.{PlanOptimizer, Rule}
import com.typesafe.config.{Config, ConfigFactory}

object LogicalPlanOptimizer extends PlanOptimizer[LogicalPlan] {

  val config: Config = ConfigFactory.empty()

  override def rules: Seq[Rule[LogicalPlan]] = Seq()

}
