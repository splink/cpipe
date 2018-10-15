import Dependencies._

enablePlugins(JavaAppPackaging)

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "xplr",
      scalaVersion := "2.12.7",
      version      := "0.1.0-SNAPSHOT"
    )),
    fork in run := true,
    name := "extr",
    libraryDependencies ++= Seq(
      logback,
      scallop,
      cassandra,
      playJson,
      scalaTest % Test,
    ),
    mainClass in Compile := Some("example.Main")
  )
