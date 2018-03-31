package org.apache.spark.example

import org.apache.spark.sql.SparkSession

object CodeGeneration extends App {

  case class Person(id: Int, firstName: String, lastName: String, companyId: Int)

  case class Company(id: Int, name: String)

  val session = SparkSession.builder()
      .master("local[1]")
      .appName("Code Generation Example")
      .getOrCreate()

  session.createDataFrame(Seq(
    Person(1, "Albert", "Dupontel", 1),
    Person(2, "Françoise", "Hardy", 1),
    Person(3, "Roger", "Moore", 2)
  )).createOrReplaceTempView("persons")

  session.createDataFrame(Seq(
    Company(1, "OCTO Technology"),
    Company(1, "Xebia")
  )).createOrReplaceTempView("companies")

  //val join = session.sql("SELECT * FROM companies c JOIN persons p ON c.id = p.companyId WHERE p.firstName = 'Albert' AND c.name = 'OCTO Technology'")
  //join.queryExecution.debug.codegen()

  val cartesianProduct = session.sql("SELECT * FROM companies c CROSS JOIN persons p")
  cartesianProduct.queryExecution.debug.codegen()

  cartesianProduct
    .foreach({ row =>
      println(row)
    })

  session.close()

}
