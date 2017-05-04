package com.octo.mythbuster.spark.sql

import java.nio.file.{Files, Paths, StandardOpenOption}

import com.google.common.base.Charsets
import com.google.common.io.{CharSource, Resources}
import com.octo.mythbuster.spark.{Logging, Resource}
import com.typesafe.config.ConfigFactory
import org.scalameter.api._

import scala.collection.JavaConverters._

object CodeGenerationBench extends QueryBench[(Int, Int)] with Logging {

  val TableRowCounts = for {
    leftTableCount <- Gen.range("Left Table Row Count")(0, 2000, 200)
    rightTableCount <- Gen.range("Right Table Row Count")(0, 30, 3)
  } yield (leftTableCount, rightTableCount)

  generateTables

  performance of "SQL Query" in {

    measure method "With Code Generation" in {

      var query: Query = null

      using(TableRowCounts)
        .setUp({ case (leftTableRowCount, rightTableRowCount) =>
          query = Query(sql((leftTableRowCount, rightTableRowCount)), Query.ConfigWithCodeGeneration).get
        })
        .in({ params =>
          println(params)
          query.fetch().foreach({ _ => })
        })
    }

    measure method "Without Code Generation" in {
      var query: Query = null
      using(TableRowCounts)
        .setUp({ case (leftTableRowCount, rightTableRowCount) =>
          query = Query(sql((leftTableRowCount, rightTableRowCount)), Query.ConfigWithoutCodeGeneration).get
        })
        .in({ params =>
          println(params)
          query.fetch().foreach({ _ => })
        })
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
        (params[Int]("Left Table Row Count"), params[Int]("Right Table Row Count"))
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
          case Some(csvFileUrl) => {
            logger.debug("Generating {} table with only {} rows", tableName, tableRowCount)
            val lines = Resources.asCharSource(csvFileUrl, Charsets.UTF_8).readLines().asScala.take(tableRowCount + 1)
            Files.write(Paths.get("target/scala-2.12/test-classes").resolve(s"${tableName}_${tableRowCount}.csv"), lines.asJava, StandardOpenOption.CREATE)
          }
          case None => throw new Exception("Unable to generate table")
        }
      })

  }

}
