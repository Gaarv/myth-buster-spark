package com.octo.mythbuster.spark.sql

import java.nio.file.{Files, Paths, StandardOpenOption}

import org.scalameter.utils.Tree
import org.scalameter.{CurveData, Persistor, Reporter}
import org.sameersingh.scalaplot._
import org.sameersingh.scalaplot.gnuplot.GnuplotPlotter

import scala.sys.process._
import scala.language.postfixOps
import scala.collection.JavaConverters._
import scala.util.Random

// http://gnuplot.sourceforge.net/demo/layout.html
// https://www.sciencetronics.com/greenphotons/?p=570

object PlotReporter {

  val FileParentFolderPath = Paths.get("target/benchmarks")

}

object Color {

  def random(count: Int, mix: Color = Color(255, 255, 255)): Seq[Color] = (1 to count) map { _ =>
    val random = new Random()
    var red = Random.nextInt(256)
    var green = random.nextInt(256)
    var blue = random.nextInt(256)

    // mix the color
    if (mix != null) {
      red = (red + mix.red) / 2
      green = (green + mix.green) / 2
      blue = (blue + mix.blue) / 2
    }

    Color(red, green, blue)
  }

}

case class Color(red: Int, green: Int, blue: Int) {

  override def toString(): String = "#%02x%02x%02x".format(red, green, blue)

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

    val params = curves(0).measurements.flatMap(_.params.axisData.keys).map(_.fullName).distinct
    val xParam = params(0)
    val yParam = params(1)

    val x = curves(0).measurements.map(_.params[Int](xParam).toDouble)
    val y = curves(0).measurements.map(_.params[Int](yParam).toDouble)

    val z = curves.map(_.measurements.map(_.value))

    val colors = Color.random(curves.size).zipWithIndex.map({ case (color, index) =>
      s"""set linetype ${index * 2 + 1} linecolor rgb "${color.toString()}" linewidth 2"""
    }).mkString("\n")

    // We write the data file
    val lines = (s"X Y ${(1 to z.size).map({ i => s"Z${i}" }).mkString(" ")}") +: (x.zip(y).zipWithIndex.map({ case ((x, y), i) => Seq(x, y) ++ z.map(_(i)) }).map(_.mkString(" ")))
    Files.write(Paths.get("target/benchmark.data"), lines.asJava, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE)

    val splot = "splot " + curves.zipWithIndex.map({ case (curve, index) =>
      s""""./target/benchmark.data" using 1:2:${index + 3} with lines title "${curve.context.scopeList(1)}""""
    }).mkString(", ")

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
         |${params.zip(Seq("x", "y")).map({ case (p, d) => s"""set ${d}label "${p}" font ",13"""" }).mkString("\n")}
         |set zlabel "Time" font ",13"
         |
         |${colors}
         |
         |set yrange []
         |set xrange [] reverse
         |
         |set dgrid3d 30,30
         |set hidden3d nooffset
         |
         |${splot}
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
