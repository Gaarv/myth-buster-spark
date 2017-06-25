lazy val commonSettings = Seq(
  organization := "com.octo.mythbuster",
  version := "1.0",
  libraryDependencies ++= Dependencies.Common,
  testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework"),
  logBuffered := false,
  fork := true,
  parallelExecution in Test := false,
  scalaVersion := "2.12.1",
  scalacOptions ++= Seq("-deprecation", "-feature")
)

lazy val root = (project in file("."))
  .settings(commonSettings)
  .settings(name := "mythbuster-spark")
  .aggregate(common, compiler, sql, example)

lazy val common = (project in file("common"))
  .settings(commonSettings)
  .settings(name := "common")

lazy val compiler = (project in file("compiler"))
  .settings(commonSettings)
  .settings(name := "compiler")
  .settings(initialCommands in console := "import com.octo.mythbuster.spark.compiler._")
  .dependsOn(common % "test->test;compile->compile")

lazy val jvmBenchmark = (project in file("jvm-benchmark"))
  .settings(name := "jvm-benchmark")
  .enablePlugins(JmhPlugin)

lazy val sql = (project in file("sql"))
  .settings(commonSettings)
  .settings(name := "sql")
  .settings(libraryDependencies ++= Dependencies.Parsing)
  .settings(initialCommands in console :=
    """
      |import com.octo.mythbuster.spark.Implicits._
      |import com.octo.mythbuster.spark.sql.lexer._
      |import com.octo.mythbuster.spark.sql.parser._
      |
      |import com.octo.mythbuster.spark.example._
    """.stripMargin)
  .dependsOn(common % "test->test;compile->compile", compiler)

lazy val example = (project in file("example"))
  .settings(commonSettings)
  .settings(name := "example")
  .dependsOn(sql)
