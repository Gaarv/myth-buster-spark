package com.octo.mythbuster.spark

import com.octo.mythbuster.spark.sql.catalyst.plans.logical.LogicalPlan
import com.octo.mythbuster.spark.sql.catalyst.plans.physical.PhysicalPlan
import com.octo.mythbuster.spark.sql.catalyst.lexer.Lexer
import com.octo.mythbuster.spark.sql.catalyst.parser.{TableColumn, Parser}
import com.octo.mythbuster.spark.sql._

object Example {

  case class Car(id: Int, name: String, companyID: Int)

  case class Company(id: Int, name: String)

  def table[A](tableName: TableName)(func: A => Map[ColumnName, Any]): Iterable[A] => Map[TableName, Iterable[Row]] = { iterable: Iterable[A] =>
    Map(tableName -> iterable.map(func(_).map({ case (columnName, value) => ((tableName, columnName), value) })))
  }

  def companyTable = table[Company]("company") { case Company(id, name) => Map("id" -> id, "name" -> name) }

  def carTable = table[Car]("car") { case Car(id, name, companyID) => Map("id" -> id, "name" -> name, "company_id" -> companyID) }

  def main(arguments: Array[String]): Unit = {

    val cars = Seq(
      Car(1, "Twingo", 1),
      Car(2, "Clio", 1),
      Car(3, "Prius", 2),
      Car(4, "Celica", 3)
    )

    val companies = Seq(
      Company(1, "Renault"),
      Company(2, "Toyota")
    )

    implicit val iterablesByTableName: Map[TableName, Iterable[Row]] = companyTable(companies) ++ carTable(cars)

    val sql = "SELECT car.name FROM company, car  WHERE car.company_id = company.id"

    val tokens = Lexer(sql)
    println("TOKENS : " + tokens)

    val ast = Parser(tokens)
    println("AST : " + ast)

    val logicalPlan = LogicalPlan(ast)
    println("LOGICAL : " + logicalPlan)

    val physicalPlan = PhysicalPlan(logicalPlan)
    println("PHYSICAL : " + physicalPlan.explain())

    val rows = physicalPlan.execute(withCodeGeneration = true)
    rows.foreach(println)
  }

}
