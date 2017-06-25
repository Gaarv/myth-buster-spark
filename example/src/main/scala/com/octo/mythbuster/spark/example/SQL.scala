package com.octo.mythbuster.spark.example

import com.octo.mythbuster.spark.sql.Query
import com.octo.mythbuster.spark.sql.Query._

import scala.util.{ Success, Failure }

object SQL extends App {

  val sql =
    """
      |SELECT
      |  *
      |FROM
      |  pedestrians_in_nation p
      |JOIN
      |  validations v
      |ON
      |      p.day = v.day
      |WHERE
      |      v.subway_station_name = 'NATION'
      |  AND p.day = '2016-12-25'
      |  AND v.validation_type = 'NAVIGO'
    """.stripMargin

  Query(sql, ConfigWithoutCodeGeneration) match {
    case Success(query) =>
      query.fetchAsCSV().foreach(println)

    case Failure(e) => e.printStackTrace(Console.out)
  }

}
