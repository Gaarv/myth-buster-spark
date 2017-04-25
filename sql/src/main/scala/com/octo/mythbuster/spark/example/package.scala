package com.octo.mythbuster.spark

import com.octo.mythbuster.spark.sql.{Row, Query}
import com.octo.mythbuster.spark.sql.plan.QueryPlanner
import com.octo.mythbuster.spark.sql.plan.logical.LogicalPlan
import com.octo.mythbuster.spark.sql.plan.physical.PhysicalPlan
import sql.parser._
import sql.lexer._

package object example extends App {

  lazy val sql: String = "SELECT a.name, o.name AS car_name FROM cars a JOIN companies o ON a.company_id = o.id WHERE o.name = 'Toyota'"
  lazy val sql2: String = "SELECT t.station, t.trafic, t.reseau, p.coord FROM positions p JOIN trafic t ON p.stop_name = t.station  WHERE t.trafic > 10000000"
  //println("SQL : " + sql)

  lazy val tokens: Seq[Token] = Lexer(sql2).get
  println("Tokens : " + tokens)

  lazy val ast: AST = Parser(tokens).get
  //println("AST : " + ast)

  lazy val logicalPlan: LogicalPlan = LogicalPlan(ast).get
  //println("Logical plan : " + logicalPlan)

  lazy val physicalPlan: PhysicalPlan = QueryPlanner.planQuery(logicalPlan).get
  //println("Physical plan : " + physicalPlan.explain())

  val startTime = System.currentTimeMillis()
  val resultIterator = Query(sql2).get.fetch()
  val startExecutionTime = System.currentTimeMillis()
  val result = resultIterator.toIndexedSeq
  val endTime = System.currentTimeMillis()
  val totalTime = endTime - startTime
  val executionTime = endTime - startExecutionTime

  showRows(result)

  println("\n")
  println("Plan compilation : " + (totalTime - executionTime) /1000 + "s")
  println("Execution time   : " + (executionTime / 1000) + "s")
  println("------------------------")
  println("Total time       : " + (totalTime / 1000) + "s")

  val test = IndexedSeq(
    Map("color" -> "yellow", "name" -> "Patrick", "message" -> "hello you"),
    Map("color" -> "blue", "name" -> "The queen elizabeth", "message" -> "hey"),
    Map("color" -> "dark green", "name" -> "Amy", "message" -> "You are all awesome")
  )

  def showRows(listRows : IndexedSeq[Row]) = {
    def pad(s : String, length : Int) = s + (" " * Math.max(length - s.length, 0))

    if(listRows.isEmpty) {
      println("No element found")
    }
    else {
      val columns = listRows.head.keys.map(_.toString).toIndexedSeq
      val results = columns.map{ column =>
        column -> listRows.map(_(column).toString)
      }.toMap
      val sizes = results.map{ case (column, elements) =>
        column -> (Seq(column) ++ elements).map(_.length).max
      }.toMap
      val paddedResults = sizes.map{ case (column, size) =>
        pad(column, size) -> results(column).map(pad(_, size))
      }.toMap
      val nbTuples = paddedResults.head._2.length
      println(paddedResults.keys.mkString(" ", " | ", " "))
      println(sizes.map(s => "-" * s._2).mkString("-", "-+-", "-"))
      println((0 until nbTuples).map(i => paddedResults.map(s => s._2(i)).mkString(" ", " | ", " ")).mkString("\n"))
    }
  }

}
