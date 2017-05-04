package com.octo.mythbuster.spark.example

import java.nio.file.{Files, Paths, StandardOpenOption}

import com.google.common.base.Charsets
import com.google.common.io.Resources
import com.octo.mythbuster.spark.{Logging, Resource}
import org.scalameter._
import org.scalameter.api._

import scala.collection.JavaConverters._

object VirtualCallsBenchmark extends App{

  trait Base {
    def A() : Float
    def B() : Int
    def C() : String
  }

  case class ClassA(i : Int) extends Base {
    override def A(): Float = i / 12.3f
    override def B(): Int = i * i
    override def C(): String = A.toString + B.toString
  }

  case class ClassB(f : Float) extends Base {
    override def A(): Float = f * f * 3 / 12.3f
    override def B(): Int = Math.sqrt(f).toInt
    override def C(): String = A.toString + B.toString
  }

  case class ClassC(s : String) extends Base {
    override def A(): Float = s.length.toFloat / (1 + s.replace("a", "").length)
    override def B(): Int = s.count(_ == 'a')
    override def C(): String = A.toString + B.toString
  }

  import org.scalameter.Warmer.Default

  val baseSeq : Seq[Base] = Seq(ClassA(3), ClassB(1.7f), ClassC("baseClass"), ClassB(3.8f))
  val timeWithVirtualCalls = config(
    Key.exec.minWarmupRuns -> 100,
    Key.exec.maxWarmupRuns -> 1000,
    Key.exec.benchRuns -> 10000
  ).withWarmer{
    Default()
  } measure {
    baseSeq.foreach{ base =>
      base.A()
      base.B()
      base.C()
    }
  }

  val a = ClassA(3)
  val b = ClassB(1.7f)
  val c = ClassC("baseClass")
  val bb = ClassB(3.8f)
  val timeWithoutVirtualCalls = config(
    Key.exec.minWarmupRuns -> 100,
    Key.exec.maxWarmupRuns -> 1000,
    Key.exec.benchRuns -> 10000,
    Key.verbose -> true
  ).withWarmer{
    Default()
  } measure {
    a.A()
    a.B()
    a.C()
    b.A()
    b.B()
    b.C()
    c.A()
    c.B()
    c.C()
    bb.A()
    bb.B()
    bb.C()
  }

  println(s"Total time with virtual : $timeWithVirtualCalls")
  println(s"Total time without virtual : $timeWithoutVirtualCalls")

}
