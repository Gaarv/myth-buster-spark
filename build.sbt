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
  .settings(name := "spark")
  .aggregate(common, compiler, sql)

lazy val common = (project in file("common"))
  .settings(commonSettings)
  .settings(name := "spark-common")

lazy val compiler = (project in file("compiler"))
  .settings(commonSettings)
  .settings(name := "spark-compiler")
  .dependsOn(common)

lazy val sql = (project in file("sql"))
  .settings(commonSettings)
  .settings(name := "spark-sql")
  .settings(libraryDependencies ++= Dependencies.Parsing)
  .dependsOn(common % "test->test,compile->compile")

