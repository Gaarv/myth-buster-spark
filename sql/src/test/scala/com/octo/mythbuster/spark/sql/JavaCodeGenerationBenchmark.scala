package com.octo.mythbuster.spark.sql

import org.scalameter.api._

/*class JavaCodeGenerationBenchmark extends Bench.LocalTime {

  val sql = """SELECT
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

  val sizes = Gen.range("size")(300000, 1500000, 300000)

  val ranges = for {
    size <- sizes
  } yield 0 until size

  performance of "JavaCodeGeneration" in {
    measure method "execute" in {
      using(ranges) in {
        val query = Query
      }
    }
  }

}*/
