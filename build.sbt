ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

libraryDependencies += "com.github.pureconfig" %% "pureconfig" % "0.17.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.0"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.12"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.12" % "test"

lazy val root = (project in file("."))
  .settings(
    name := "flight-data"
  )
