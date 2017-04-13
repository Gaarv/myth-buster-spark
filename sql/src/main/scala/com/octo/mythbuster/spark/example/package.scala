package com.octo.mythbuster.spark

import com.octo.mythbuster.spark.sql.plan.QueryPlanner
import com.octo.mythbuster.spark.sql.plan.logical.LogicalPlan
import com.octo.mythbuster.spark.sql.plan.physical.PhysicalPlan
import sql.parser._
import sql.lexer._

package object example {

  lazy val sql: String = "SELECT a.name AS car_name FROM cars a JOIN companies o ON a.company_id = o.id WHERE o.name = 'Toyota'"

  lazy val tokens: Seq[Token] = Lexer(sql).get

  lazy val ast: AST = Parser(tokens).get

  lazy val logicalPlan: LogicalPlan = LogicalPlan(ast).get

  lazy val physicalPlan: PhysicalPlan = QueryPlanner.planQuery(logicalPlan).get

}
