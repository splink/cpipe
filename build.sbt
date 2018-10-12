import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "xplr",
      scalaVersion := "2.12.7",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "extr",
    libraryDependencies ++= Seq(
      cassandra,
      playJson,
      scalaLogging,
      scalaTest % Test,
    )
  )
