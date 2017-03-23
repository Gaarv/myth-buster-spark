package com.octo.mythbuster.spark.sql

import scala.util.{ Failure, Success }
import com.octo.mythbuster.spark.UnitSpec

class QuerySpec extends UnitSpec {

  import sample._

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

  it should "return only 2 rows" in {
    query match {
      case Success(query) => {
        val result = query.fetch().toSeq
        result.length should be(2)
      }
      case Failure(e) => fail(e)
    }
  }

}
