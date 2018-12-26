import Dependencies._
import scala.sys.process._

lazy val root = (project in file(".")).
  enablePlugins(BuildInfoPlugin).
  enablePlugins(JavaAppPackaging).
  settings(
    List(
      maintainer := "maxmc",
      organization := "splink",
      scalaVersion := "2.12.7",
      buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion, "commitId" -> "git rev-parse HEAD".!!.slice(0, 10)),
      buildInfoPackage := "org.splink.cpipe.config"
    ) ++ releaseSettings ++ githubReleaseSettings,
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

import ReleaseTransformations._

lazy val releaseSettings = Seq(
  releaseProcess := Seq[ReleaseStep](
    checkSnapshotDependencies,
    inquireVersions,
    runClean,
    runTest,
    setReleaseVersion,
    commitReleaseVersion,
    tagRelease,
    pushChanges,
    releaseStepCommand("universal:packageBin"),
    releaseStepCommand("githubRelease"),
    setNextVersion,
    commitNextVersion,
    pushChanges,
  )
)

lazy val githubReleaseSettings = Seq(
  ghreleaseRepoOrg := organization.value,
  ghreleaseRepoName := name.value,
  ghreleaseAssets := Seq(
    target.value / "universal" / s"${name.value}-${version.value}.zip"
  ),
  ghreleaseNotes := { tagName ⇒
    SimpleReader.readLine(s"Input release notes for $tagName: ").getOrElse("")
  }
)