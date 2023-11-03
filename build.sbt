ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.11"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.16" % "test"

libraryDependencies += "com.opencsv" % "opencsv" % "5.7.1"

// need to confirm the version of spark and hadoop
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.1"