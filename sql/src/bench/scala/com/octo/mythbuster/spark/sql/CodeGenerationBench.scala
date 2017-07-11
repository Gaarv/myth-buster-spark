package com.octo.mythbuster.spark.sql;

import org.scalameter.Gen
import java.nio.file.{ Paths, Files, StandardOpenOption }
import com.octo.mythbuster.spark.{ Logging, Resource }
import com.google.common.io.Resources
import com.google.common.base.Charsets
import scala.collection.JavaConverters._

object CodeGenerationBench extends QueryBench[(Int, Int)] with Logging {

  val TableRowCounts = for {
    leftTableCount <- Gen.range("Left Table Row Count")(0, 6000, 500)
    rightTableCount <- Gen.range("Right Table Row Count")(0, 200000, 50000)
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
          query.fetch().foreach({ _ => })
        })
    }

    measure method "Without Code Generation" in {
      var query: Query = null
      using(TableRowCounts)
        .setUp({ case (leftTableRowCount, rightTableRowCount) =>
          val generatedSQL = sql((leftTableRowCount, rightTableRowCount))
          println(generatedSQL)
          query = Query(generatedSQL, Query.ConfigWithoutCodeGeneration).get
        })
        .in({ params =>
          query.fetch().foreach({ _ => })
        })
    }
  }

  def sql = { case (leftTableCount, rightTableCount) =>
      s"""
        |SELECT
        |  p.pedestrian_count,
        |  v.subway_station_name
        |FROM
        |  pedestrians_in_nation_${leftTableCount} p
        |JOIN
        |  validations_${rightTableCount} v
        |ON
        |      p.day = v.day
        |WHERE
        |      v.subway_station_name = 'NATION'
        |  AND p.day = '2016-12-25'
        |  AND v.validation_type = 'NAVIGO'
      """.stripMargin
  }

  private def generateTables: Unit = {
    TableRowCounts.dataset
      .map({ params =>
        (params[Int]("Left Table Row Count"), params[Int]("Right Table Row Count"))
      })
      .flatMap({ case (leftTableRowCount, rightTableRowCount) =>
          Seq(
            ("pedestrians_in_nation", leftTableRowCount),
            ("validations", rightTableRowCount)
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
