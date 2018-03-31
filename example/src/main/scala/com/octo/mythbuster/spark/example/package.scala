package com.octo.mythbuster.spark

import com.octo.mythbuster.spark.sql.lexer.Lexer
import com.octo.mythbuster.spark.sql.parser.Parser
import com.octo.mythbuster.spark.sql.plan.QueryPlanner
import com.octo.mythbuster.spark.sql.plan.logical.LogicalPlan
import com.octo.mythbuster.spark.sql.plan.physical.codegen.{JavaCodeGeneration, JavaCodeGenerationContext}
import com.octo.mythbuster.spark.sql.plan.physical.{PhysicalPlan, PhysicalPlanOptimizer}
import com.typesafe.config.Config

import scala.util.{Failure, Success}

package object example {

  val SQL =
    """
      |SELECT
      |  p.pedestrian_count,
      |  v.subway_station_name
      |FROM
      |  pedestrians_in_nation p
      |JOIN
      |  validations v
      |ON
      |      p.day = v.day
      |WHERE
      |      v.subway_station_name = 'NATION'
      |  AND p.day = '2016-12-25'
      |  AND v.validation_type = 'NAVIGO'
    """.stripMargin

  def logicalPlanOf(sql: String): LogicalPlan = {
    (for {
      tokens <- Lexer(sql)
      ast <- Parser(tokens)
      logicalPlan <- LogicalPlan(ast)
    } yield logicalPlan) match {
      case Success(logicalPlan) =>
        logicalPlan
      case Failure(e) =>
        throw e
    }
  }

  def physicalPlanOf(sql: String): PhysicalPlan = {
    QueryPlanner.planQuery(logicalPlanOf(sql)) match {
      case Success(physicalPlan) =>
        physicalPlan

      case Failure(e) =>
        throw e
    }
  }

  def optimize(physicalPlan: PhysicalPlan, config: Config): PhysicalPlan = {
    PhysicalPlanOptimizer(config).optimizePlan(physicalPlan)
  }

  def generateJavaCode(physicalPlan: PhysicalPlan): String = physicalPlan match {
    case javaCodeGeneration: JavaCodeGeneration =>
      javaCodeGeneration.generateJavaCode(JavaCodeGenerationContext())

    case _ =>
      throw new UnsupportedOperationException()
  }

}
