package com.octo.mythbuster.spark.sql

import com.octo.mythbuster.spark.sql.CodeGenerationBenchmark.sql
import com.typesafe.config.{Config, ConfigFactory}
import org.scalameter.Reporter.Composite
import org.scalameter.{Aggregator, Measurer}
import org.scalameter.api.Bench.Local
import org.scalameter.api.Reporter
import org.scalameter.picklers.Implicits.doublePickler
import org.scalameter.reporting.DsvReporter

import scala.util.{Failure, Success}

abstract class QueryBench[P] extends Local[Double] {

  override def aggregator: Aggregator[Double] = Aggregator.average

  def measurer: Measurer[Double] = new Measurer.IgnoringGC
    with Measurer.PeriodicReinstantiation[Double]
    with Measurer.OutlierElimination[Double]
    with Measurer.RelativeNoise {
    def numeric: Numeric[Double] = implicitly[Numeric[Double]]
  }

  override def reporter: Reporter[Double] = Composite(PlotReporter())

  def sql: P => String

  def query(config: Config = ConfigFactory.load()): P => Unit = { params =>
    Query(sql(params), config) match {
      case Success(query) => query
      case Failure(e) => throw e
    }
  }

}
