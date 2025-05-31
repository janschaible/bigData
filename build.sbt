ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

// %% adds the scala version to the path...
ThisBuild / libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.5"
ThisBuild / libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.5.5"
ThisBuild / libraryDependencies += "org.apache.spark" %% "spark-graphx" % "3.5.5"
ThisBuild / libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.19.0"
ThisBuild / libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.19.0"
ThisBuild / libraryDependencies += "com.github.javafaker" % "javafaker" % "1.0.2"


lazy val root = (project in file("."))
  .settings(
    name := "bigData"
  )
