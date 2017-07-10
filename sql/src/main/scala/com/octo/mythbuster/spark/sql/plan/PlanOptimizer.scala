package com.octo.mythbuster.spark.sql.plan

import com.octo.mythbuster.spark.Logging
import com.typesafe.config.Config

trait PlanOptimizer[P <: Plan[P]] extends Logging {

  val config: Config

  def rules: Seq[Rule[P]]

  def optimizePlan(plan: P): P = rules.foldLeft(plan) { (plan, rule) =>
    logger.info("Applying {} rule", rule.name)
    rule(plan)
  }

  def apply(plan: P): P = optimizePlan(plan)

}
