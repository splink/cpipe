import Dependencies._

enablePlugins(JavaAppPackaging)

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "splink",
      scalaVersion := "2.12.7",
      version      := "0.1.0-SNAPSHOT"
    )),
    fork in run := true,
    name := "cpipe",
    libraryDependencies ++= Seq(
      logback,
      scallop,
      cassandra,
      lz4,
      playJson,
      scalaTest % Test,
    ),
    mainClass in Compile := Some("org.splink.cpipe.CPipe")
  )
