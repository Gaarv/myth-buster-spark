package com.octo.mythbuster.spark.sql.plan

import com.typesafe.config.Config

trait PlanOptimizer[P <: Plan[P]] {

  def rules(implicit config: Config): Seq[Rule[P]]

  def optimizePlan(plan: P): P = rules.foldLeft(plan) { (plan, rule) =>
    rule(plan)
  }

  def apply(plan: P): P = optimizePlan(plan)

}
