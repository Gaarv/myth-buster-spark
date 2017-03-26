package com.octo.mythbuster.spark.sql.plan.logical

import com.octo.mythbuster.spark.sql.plan.{PlanOptimizer, Rule}
import com.typesafe.config.Config

object LogicalPlanOptimizer extends PlanOptimizer[LogicalPlan] {

  override def rules(implicit config: Config): Seq[Rule[LogicalPlan]] = Seq()

}
