package com.octo.mythbuster.spark.sql

import scala.util.{Failure, Success}
import com.octo.mythbuster.spark.UnitSpec
import com.typesafe.config.{ConfigFactory, ConfigValue, ConfigValueFactory}

class QuerySpec extends UnitSpec {

  import sample._

  def config(shouldGenerateCode: Boolean) = {
    ConfigFactory.empty()
      .withValue("shouldGenerateCode", ConfigValueFactory.fromAnyRef(shouldGenerateCode))
  }

  val sql =
    """
      |SELECT
      |  ca.name AS name
      |FROM
      |  cars ca
      |JOIN
      |  companies co
      |ON
      |  ca.company_id = co.id
      |WHERE
      |  co.name = 'Toyota'
    """.stripMargin

  "The query" should "produce a valid explain plan" in {
    Query(sql, config(true)) match {
      case Success(query) => {
        println(query.explain())
      }
      case Failure(e) => fail(e)
    }
  }

  it should "return only 2 rows with code generation" in {
    Query(sql, config(true)) match {
      case Success(query) => {
        val result = query.fetch().toSeq
        result.foreach(println)
        result.length should be(2)
      }
      case Failure(e) => fail(e)
    }
  }

  it should "also return only 2 rows without code generation" in {
    implicit val configWithoutCodeGeneration = ConfigFactory.empty()
      .withValue("shouldGenerateCode", ConfigValueFactory.fromAnyRef(false))

    Query(sql, config(false)) match {
      case Success(query) => {
        println(query.physicalPlan)

        val queryWithoutCodeGeneration = query

        val result = queryWithoutCodeGeneration.fetch().toSeq
        result.foreach(println)
        result.length should be(2)
      }
      case Failure(e) => fail(e)
    }
  }

}
