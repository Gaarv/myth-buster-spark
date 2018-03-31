package com.octo.mythbuster.spark.sql.plan

import com.octo.mythbuster.spark.Logging
import com.typesafe.config.Config

object PlanOptimizer {

  val MaxIterations: Int = 5

}

trait PlanOptimizer[P <: Plan[P]] extends Logging {

  import PlanOptimizer._

  val config: Config

  def rules: Seq[Rule[P]]

  private def applyRules(plan: P): P = rules.foldLeft(plan) { (plan, rule) =>
    logger.info("Applying {} rule", rule.name)
    rule(plan)
  }

  def optimizePlan(plan: P): P = Function.chain(Seq.fill(MaxIterations)(applyRules _))(plan)

}
