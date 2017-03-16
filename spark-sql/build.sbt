name := "Java Class Compiler"

version := "1.0"

scalaVersion := "2.12.0"
scalacOptions ++= Seq("-deprecation", "-feature")

libraryDependencies += "com.google.guava" % "guava" % "20.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
libraryDependencies += "com.storm-enroute" %% "scalameter" % "0.8.2" % "test"
libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.5"
libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.12.1"

val logback = Seq(
  "ch.qos.logback" % "logback-core" % "1.2.1",
  "ch.qos.logback" % "logback-classic" % "1.2.1"
)

val slf4j = Seq(
  "org.slf4j" % "slf4j-api" % "1.7.24"
)

val logging = logback ++ slf4j

libraryDependencies ++= logging


testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework")

logBuffered := false

fork := true

parallelExecution in Test := false

//mainClass in (Compile, run) := Some("com.octo.nad.JavaClassCompilerExample")
