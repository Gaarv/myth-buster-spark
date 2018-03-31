package com.octo.mythbuster.spark.example

import com.octo.mythbuster.spark.sql._
import com.octo.mythbuster.spark.sql.lexer._
import com.octo.mythbuster.spark.sql.parser._
import com.octo.mythbuster.spark.sql.plan.QueryPlanner
import com.octo.mythbuster.spark.sql.plan.logical.{LogicalPlan, LogicalPlanOptimizer}
import com.octo.mythbuster.spark.sql.plan.physical.PhysicalPlanOptimizer

import sext._

import scala.util.{Failure, Success}

object Show extends App {

  val config = Query.ConfigWithCodeGeneration

  val sql = {
    """
      |SELECT
      |  firstName,
      |  lastName,
      |  name
      |FROM
      |  (
      |    SELECT
      |      companyId,
      |      firstName,
      |      lastName
      |    FROM
      |      employees
      |    WHERE
      |      isSpeaking = TRUE
      |  )
      |JOIN
      |  companies
      |ON
      |  companyId = id
      |WHERE
      |  firstName = 'Adrien'
    """.stripMargin
  }

  println(" COUCOU ")
  (for {
    tokens <- Lexer(sql)
    _ = println(tokens)

    ast <- Parser(tokens)
    _ = println(ast.treeString)

    logicalPlan <- LogicalPlan(ast)
    _ = println(s"Logical Plan: ${logicalPlan.treeString}")

    optimizedLogicalPlan = LogicalPlanOptimizer(config).optimizePlan(logicalPlan)
    _ = println(s"Optimized Logical Plan: ${optimizedLogicalPlan.treeString}")

    physicalPlan <- QueryPlanner.planQuery(optimizedLogicalPlan)
    _ = println(s"Physical Plan: ${physicalPlan.treeString}")

    optimizedPhysicalPlan = PhysicalPlanOptimizer(config).optimizePlan(physicalPlan)
    _ = println(s"Optimized Physical Plan: ${optimizedPhysicalPlan}")

    rows = optimizedPhysicalPlan.execute()
  } yield rows) match {
    case Success(rows) =>
      rows.foreach(println)

    case Failure(throwable) =>
      throwable.printStackTrace()

  }

}
