import sbt._

object Dependencies {

  val Guava = Seq(
    "com.google.guava" % "guava" % "20.0"
  )

  val ScalaReflect = Seq(
    "org.scala-lang" % "scala-reflect" % "2.12.1"
  )

  val Tooling = Guava ++ ScalaReflect

  val ScalaParserCombinator = Seq(
    "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.5"
  )

  val Parsing = ScalaParserCombinator

  val Logback = Seq(
    "ch.qos.logback" % "logback-core" % "1.2.1",
    "ch.qos.logback" % "logback-classic" % "1.2.1"
  )

  val SLF4J = Seq(
    "org.slf4j" % "slf4j-api" % "1.7.24"
  )

  val Logging = Logback ++ SLF4J

  val ScalaMeter = Seq(
    "com.storm-enroute" %% "scalameter" % "0.8.2"
  )

  val Config = Seq(
    "com.typesafe" % "config" % "1.3.1"
  )

  val Configuring = Config

  val ScalaTest = Seq(
    "org.scalatest" %% "scalatest" % "3.0.1"
  )

  val Testing = ScalaTest ++ ScalaMeter

  val Common = Tooling ++ Logging ++ Configuring ++ (Testing map { _ % "test" })

}