ThisBuild / scalaVersion     := "2.13.12"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "io.github"
ThisBuild / organizationName := "io/github/antonkw"

lazy val root = (project in file("."))
  .settings(
    name := "rec-cte-demo"
  )

// https://mvnrepository.com/artifact/org.apache.calcite/calcite-core
libraryDependencies += "org.apache.calcite" % "calcite-core" % "1.39.0"

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
