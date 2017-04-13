package com.octo.mythbuster.spark

import com.octo.mythbuster.spark.sql.plan.QueryPlanner
import com.octo.mythbuster.spark.sql.plan.logical.LogicalPlan
import sql.parser._
import sql.lexer._

package object example {

  lazy val sql = "SELECT a.name AS car_name FROM cars a JOIN companies o ON a.company_id = o.id WHERE o.name = 'Toyota'"

  lazy val tokens = Lexer(sql).get

  lazy val ast = Parser(tokens).get

  lazy val logicalPlan = LogicalPlan(ast).get

  lazy val physicalPlan = QueryPlanner.planQuery(logicalPlan).get

}
