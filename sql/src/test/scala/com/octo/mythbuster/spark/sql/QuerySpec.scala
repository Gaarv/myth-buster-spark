package com.octo.mythbuster.spark.sql

import scala.util.{Failure, Success}
import com.octo.mythbuster.spark.UnitSpec
import com.typesafe.config.{ConfigFactory, ConfigValue, ConfigValueFactory}

class QuerySpec extends UnitSpec {

  import sample._

  implicit val config = ConfigFactory.empty()
    .withValue("generateCode", ConfigValueFactory.fromAnyRef(true))

  val sql =
    """
      |SELECT
      |  ca.name
      |FROM
      |  cars ca
      |JOIN
      |  companies co
      |ON
      |  ca.company_id = co.id
      |WHERE
      |  co.name = 'Toyota'
    """.stripMargin

  val query = Query(sql)

  "The query" should "produce a valid explain plan" in {
    query match {
      case Success(query) => {
        println(query.explain())
      }
      case Failure(e) => fail(e)
    }
  }

  it should "return only 2 rows with code generation" in {
    query match {
      case Success(query) => {
        val result = query.fetch().toSeq
        result.length should be(2)
      }
      case Failure(e) => fail(e)
    }
  }

  it should "also return only 2 rows without code generation" in {
    //implicit val configWithoutCodeGeneration = ConfigFactory.empty()
    //  .withValue("generateCode", ConfigValueFactory.fromAnyRef(false))

    query match {
      case Success(query) => {
        val queryWithoutCodeGeneration = query

        val result = queryWithoutCodeGeneration.fetch().toSeq
        result.length should be(2)
      }
      case Failure(e) => fail(e)
    }
  }

}
