lazy val root = (project in file("."))
  .settings(name := "spark-example")
  .settings(scalaVersion := "2.11.8")
  .settings(libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.1.1",
    "org.apache.spark" %% "spark-sql" % "2.1.1"
  ))
