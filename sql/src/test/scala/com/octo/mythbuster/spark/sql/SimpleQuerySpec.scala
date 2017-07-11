package com.octo.mythbuster.spark.sql

import scala.util.{Failure, Success}
import com.octo.mythbuster.spark.UnitSpec
import com.typesafe.config.{ConfigFactory, ConfigValue, ConfigValueFactory}

class SimpleQuerySpec extends UnitSpec {

  def config(shouldGenerateCode: Boolean) = {
    ConfigFactory.empty()
      .withValue("shouldGenerateCode", ConfigValueFactory.fromAnyRef(shouldGenerateCode))
      .withValue("shouldUseHashJoin", ConfigValueFactory.fromAnyRef(true))
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

  var resultWithCodeGeneration: Seq[Row] = _
  var resultWithoutCodeGeneration: Seq[Row] = _

  "The query" should "produce a valid explain plan" in {
    Query(sql, config(true)) match {
      case Success(query) => {
        println(query.explain())
      }
      case Failure(e) =>
        e.printStackTrace(Console.err)
        fail(e)
    }
  }

  it should "return only 2 rows with code generation" in {
    Query(sql, config(true)) match {
      case Success(query) => {
        resultWithCodeGeneration = query.fetch().toSeq
        resultWithCodeGeneration.foreach(println)
        resultWithCodeGeneration.length should be(2)
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

        resultWithoutCodeGeneration = queryWithoutCodeGeneration.fetch().toSeq
        resultWithoutCodeGeneration.foreach(println)
        resultWithoutCodeGeneration.length should be(2)
      }
      case Failure(e) => fail(e)
    }
  }

  it should "return the same rows" in {
    resultWithoutCodeGeneration.toSet should equal(resultWithCodeGeneration.toSet)
  }

}
