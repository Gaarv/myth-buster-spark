package com.octo.mythbuster.spark.sql

import com.octo.mythbuster.spark.sql.plan.physical.{ PhysicalPlan, PhysicalPlanOptimizer }
import com.octo.mythbuster.spark.sql.parser.Parser
import com.octo.mythbuster.spark.sql.lexer.Lexer
import com.octo.mythbuster.spark.sql.plan.logical.{ LogicalPlan, LogicalPlanOptimizer }
import com.octo.mythbuster.spark.sql.plan.QueryPlanner

import scala.util.{ Failure, Success, Try }

object Query {

  def apply(sql: String)(implicit tableRegistry: TableRegistry): Try[Query] = for {
    tokens <- Lexer(sql)
    ast <- Parser(tokens)
    logicalPlan <- LogicalPlan(ast)
    optimizedLogicalPlan = LogicalPlanOptimizer.optimizePlan(logicalPlan)
    physicalPlan <- QueryPlanner.planQuery(optimizedLogicalPlan)
    optimizedPhysicalPlan = PhysicalPlanOptimizer.optimizePlan(physicalPlan)
    _ = println(optimizedPhysicalPlan)
  } yield new Query(optimizedPhysicalPlan, tableRegistry)

}

class Query(physicalPlan: PhysicalPlan, tableRegistry: TableRegistry) {

  def fetch(): Iterator[Row] = physicalPlan.execute().map(_.toRow)

}
