package com.octo.mythbuster.spark.sql

import com.octo.mythbuster.spark.Logging
import com.octo.mythbuster.spark.sql.parser.Parser
import com.octo.mythbuster.spark.sql.lexer.Lexer
import com.octo.mythbuster.spark.sql.plan.physical.{PhysicalPlan, PhysicalPlanOptimizer}
import com.octo.mythbuster.spark.sql.plan.logical.{LogicalPlan, LogicalPlanOptimizer}
import com.octo.mythbuster.spark.sql.plan.QueryPlanner
import com.octo.mythbuster.spark.sql.plan.physical._
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}

import scala.util.Try

object Query extends Logging {

  val ConfigWithCodeGeneration = ConfigFactory.load().withValue("shouldGenerateCode", ConfigValueFactory.fromAnyRef(false))

  val ConfigWithoutCodeGeneration = ConfigFactory.load().withValue("shouldGenerateCode", ConfigValueFactory.fromAnyRef(false))

  def apply(sql: String, config: Config = ConfigFactory.load()): Try[Query] = for {
    // We first lex the SQL query
    tokens <- Lexer(sql)
    _ = logger.info("Lexed tokens are {}", tokens)
    // With all the lexed tokens, we can generate the AST
    ast <- Parser(tokens)
    _ = logger.info("Parsed AST is {}", ast)
    // Now that we have the AST, we're able to have a logical plan...
    logicalPlan <- LogicalPlan(ast)
    _ = logger.info("Computed logical plan is {}", logicalPlan)
    // ...Which can be optimized
    optimizedLogicalPlan = LogicalPlanOptimizer.optimizePlan(logicalPlan)
    // With the logical plan, we're now able to plan the physical plan...
    physicalPlan <- QueryPlanner.planQuery(optimizedLogicalPlan)
    _ = logger.info("Planned physical plan is {}", physicalPlan)
    // ...Which can also be optimized
    optimizedPhysicalPlan = PhysicalPlanOptimizer(config).optimizePlan(physicalPlan)

  // So we can yield the Query instance, which is just a container around the final physical plan
  } yield new Query(optimizedPhysicalPlan)

}

class Query(val physicalPlan: PhysicalPlan) {

  val CSVSeparator = ";"

  // It's just a nice API to execute the physical plan we infered above and map the InteralRows to Rows
  def fetch(): Iterator[Row] = physicalPlan.execute().map(_.toRow)

  def fetchAsCSV(): Iterator[String] = fetch().zipWithIndex.flatMap({
    case (row, index) if index == 0 =>
      Seq(
        row.keys.mkString(CSVSeparator),
        row.values.mkString(CSVSeparator)
      )

    case (row, _) => Seq(row.values.mkString(CSVSeparator))
  })

  // It returns a String to have an idea of the physical plan tree which are going to be executed
  def explain(): String = physicalPlan.explain()

}
