package com.octo.mythbuster.spark.sql

import com.octo.mythbuster.spark.sql.parser.Parser
import com.octo.mythbuster.spark.sql.lexer.Lexer
import com.octo.mythbuster.spark.sql.plan.physical.{PhysicalPlan, PhysicalPlanOptimizer}
import com.octo.mythbuster.spark.sql.plan.logical.{LogicalPlan, LogicalPlanOptimizer}
import com.octo.mythbuster.spark.sql.plan.QueryPlanner
import com.octo.mythbuster.spark.sql.plan.physical._
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}

import scala.util.Try

object Query {

  val ConfigWithCodeGeneration = ConfigFactory.load().withValue("shouldGenerateCode", ConfigValueFactory.fromAnyRef(true))

  val ConfigWithoutCodeGeneration = ConfigFactory.load().withValue("shouldGenerateCode", ConfigValueFactory.fromAnyRef(false))

  def apply(sql: String, config: Config = ConfigFactory.load()): Try[Query] = for {
    // We first lex the SQL query
    tokens <- Lexer(sql)
    // With all the lexed tokens, we can generate the AST
    ast <- Parser(tokens)
    // Now that we have the AST, we're able to have a logical plan...
    logicalPlan <- LogicalPlan(ast)
    // ...Which can be optimized
    optimizedLogicalPlan = LogicalPlanOptimizer.optimizePlan(logicalPlan)
    // With the logical plan, we're now able to plan the physical plan...
    physicalPlan <- QueryPlanner.planQuery(optimizedLogicalPlan)
    // ...Which can also be optimized
    optimizedPhysicalPlan = PhysicalPlanOptimizer(config).optimizePlan(physicalPlan)

  // So we can yield the Query instance, which is just a container around the final physical plan
  } yield new Query(optimizedPhysicalPlan)

}

class Query(val physicalPlan: PhysicalPlan) {

  // It's just a nice API to execute the physical plan we infered above and map the InteralRows to Rows
  def fetch(): Iterator[Row] = physicalPlan.execute().map(_.toRow)

  // It returns a String to have an idea of the physical plan tree which are going to be executed
  def explain(): String = physicalPlan.explain()

}
