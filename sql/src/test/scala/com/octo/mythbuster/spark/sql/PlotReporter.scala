package com.octo.mythbuster.spark.sql

import java.nio.file.{Files, Paths, StandardOpenOption}

import org.scalameter.utils.Tree
import org.scalameter.{CurveData, Persistor, Reporter}
import org.sameersingh.scalaplot._
import org.sameersingh.scalaplot.gnuplot.GnuplotPlotter
import scala.sys.process._

import scala.language.postfixOps
import scala.collection.JavaConverters._

object PlotReporter {

  val FileParentFolderPath = Paths.get("target/benchmarks")

}

case class PlotReporter(fileName: String = "plot.png") extends Reporter[Double] {

  def report(result: CurveData[Double], persistor: Persistor): Unit = {
    // Nothing to do as we generate the chart in the end
  }

  private def flatten(results: Tree[CurveData[Double]]): Seq[CurveData[Double]] = {
    import collection.mutable.ListBuffer
    val buffer = ListBuffer[CurveData[Double]]()
    results.foreach({ curve =>
      buffer += curve
    })
    Seq(buffer: _*)
  }

  def report(results: Tree[CurveData[Double]], persistor: Persistor): Boolean = {
    val curves = flatten(results)

    val x = curves(0).measurements.map(_.params[Int]("leftTableRowCount").toDouble)
    val y = curves(0).measurements.map(_.params[Int]("rightTableRowCount").toDouble)

    val z1 = curves(0).measurements.map(_.value)
    val z2 = curves(1).measurements.map(_.value)

    // We write the data file
    val lines = "X Y Z1 Z2 " +: (x.zip(y).zip(z1).zip(z2).map({ case (((x, y), z1), z2) => Seq(x, y, z1, z2) }).map(_.mkString(" ")))
    Files.write(Paths.get("target/benchmark.data"), lines.asJava, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE)

    // We write the GnuPlot script
    val script =
      s"""
         |set terminal pngcairo enhanced font "ubuntu,10" fontscale 1.0 size 800, 600 background 'white' linewidth 1
         |set output './target/benchmark.png'
         |
         |set grid linetype 1 linecolor rgb "#e0e0e0"
         |set border linewidth 1 linecolor rgb "#808080"
         |set tics textcolor rgb "#505050"
         |set key font ",13" spacing 2
         |
         |set style fill   solid 1.00 noborder
         |set style function filledcurves y1=0
         |
         |set title "${titleOf(results)}" font ",15"
         |set xlabel "Left Table Row Count" font ",13"
         |set ylabel "Right Table Row Count" font ",13"
         |set zlabel "Time" font ",13"
         |
         |set linetype 1 linecolor rgb "#3923D6" linewidth 2
         |set linetype 3 linecolor rgb "#2DC800" linewidth 2
         |
         |set dgrid3d 30,30
         |set hidden3d nooffset
         |
         |splot "./target/benchmark.data" using 1:2:3 with lines title "With Code Generation", "./target/benchmark.data" using 1:2:4 with lines title "Without Code Generation"
         |
       """.stripMargin

    Files.write(Paths.get("target/benchmark.gnuplot"), Seq(script).asJava, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE)
    val exitCode = Seq("gnuplot", "./target/benchmark.gnuplot") !

    exitCode == 0
  }

  private def titleOf(results: Tree[CurveData[Double]]): String = {
    flatten(results).map(_.context.scopeList.head).distinct.head
  }

  private def keyOf(curve: CurveData[Double]): String = {
    curve.measurements.map(_.params.axisData).flatMap(_.keys).map(_.fullName).distinct.head
  }

}
