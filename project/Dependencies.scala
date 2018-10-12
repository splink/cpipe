import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5"
  lazy val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
  lazy val playJson = "com.typesafe.play" %% "play-json" % "2.6.9"
  lazy val cassandra = "com.datastax.cassandra" % "cassandra-driver-core" % "3.6.0"
}
