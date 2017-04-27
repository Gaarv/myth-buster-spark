package com.octo.mythbuster.spark.sql

import java.nio.file.{Files, Paths, StandardOpenOption}

import com.google.common.base.Charsets
import com.google.common.io.Resources
import com.octo.mythbuster.spark.{Logging, Resource}
import com.typesafe.config.ConfigFactory
import org.scalameter.api._

import scala.collection.JavaConverters._

object CodeGenerationBench extends QueryBench[(Int, Int)] with Logging {

  val TableRowCounts = for {
    leftTableCount <- Gen.range("leftTableRowCount")(0, 10000, 500)
    rightTableCount <- Gen.range("rightTableRowCount")(0, 300, 15)
  } yield (leftTableCount, rightTableCount)



  performance of "Code" in {

    generateTables

    measure method "With Code Generation" in {
      using(TableRowCounts) in query(Query.ConfigWithCodeGeneration)
    }

    measure method "Without Code Generation" in {
      using(TableRowCounts) in query(Query.ConfigWithoutCodeGeneration)
    }
  }

  def sql = { case (leftTableCount, rightTableCount) =>
    s"""
       |SELECT
       |  t.station,
       |  t.trafic,
       |  t.reseau,
       |  p.coord
       |FROM
       |  positions_${leftTableCount} p
       |JOIN
       |  trafic_${rightTableCount} t
       |ON
       |  p.stop_name = t.station
     """.stripMargin
  }

  private def generateTables: Unit = {
    TableRowCounts.dataset
      .map({ params =>
        (params[Int]("leftTableRowCount"), params[Int]("rightTableRowCount"))
      })
      .flatMap({ case (leftTableRowCount, rightTableRowCount) =>
          Seq(
            ("positions", leftTableRowCount),
            ("trafic", rightTableRowCount)
          )
      })
      .filter({ case (tableName, tableRowCount) =>
        Resource(s"${tableName}_${tableRowCount}.csv").isEmpty
      })
      .foreach({ case (tableName, tableRowCount) =>
        Resource(s"${tableName}.csv") match {
          case Some(csvFileURL) => {
            logger.debug("Generating {} table with only {} rows", tableName, tableRowCount)
            val lines = Resources.asCharSource(csvFileURL, Charsets.UTF_8).readLines().asScala.take(tableRowCount + 1).asJava
            Files.write(Paths.get("target/scala-2.12/test-classes").resolve(s"${tableName}_${tableRowCount}.csv"), lines, StandardOpenOption.CREATE)
          }
          case None => throw new Exception("Unable to generate table")
        }
      })

  }

}
