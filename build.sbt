ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.11"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.1"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.16" % "test"
