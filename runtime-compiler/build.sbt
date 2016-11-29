name := "runtime-compiler"

version := "1.0"

scalaVersion := "2.12.0"

libraryDependencies += "com.google.guava" % "guava" % "20.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
libraryDependencies += "com.storm-enroute" %% "scalameter" % "0.8.2" % "test"

testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework")

logBuffered := false

fork in Test := true

parallelExecution in Test := false