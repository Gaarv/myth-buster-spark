package com.octo.mythbuster.spark.sql.plan.logical

import com.octo.mythbuster.spark.sql.plan.physical.PhysicalPlanOptimizer
import com.octo.mythbuster.spark.sql.plan.{PlanOptimizer, Rule}
import com.typesafe.config.{Config, ConfigFactory}

import rules._

object LogicalPlanOptimizer {

  def apply(config: Config) = new LogicalPlanOptimizer(config)

}

class LogicalPlanOptimizer(val config: Config) extends PlanOptimizer[LogicalPlan] {

  override def rules: Seq[Rule[LogicalPlan]] = Seq(PushDownFilter, CombineFilters, SwitchProjectionAndFilter)

}
