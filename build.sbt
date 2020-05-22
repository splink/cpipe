import Dependencies._
import scala.sys.process._

lazy val root = (project in file(".")).
  enablePlugins(BuildInfoPlugin).
  enablePlugins(JavaAppPackaging, RpmPlugin, DebianPlugin).
  settings(
    List(
      maintainer := "maxmc",
      organization := "splink",
      scalaVersion := "2.12.8",
      buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion, "commitId" -> "git rev-parse HEAD".!!.slice(0, 10)),
      buildInfoPackage := "org.splink.cpipe.config"
    ) ++ releaseSettings ++ githubReleaseSettings,
    fork in run := true,
    name := "cpipe",
    rpmVendor := "splink",
    rpmPackager := Some("splink"),
    packageSummary := "Import/Export rows from a Cassandra database.",
    packageDescription := "Import/Export rows from a Cassandra database. Run 'cpipe --help' for more information.",
    rpmLicense := Some("Apache License Version 2.0"),
    packageName := "cpipe",
    packageArchitecture := "noarch",
    debianPackageDependencies := Seq("java8-runtime-headless"),
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
    releaseStepCommand("debian:packageBin"),
    releaseStepCommand("rpm:packageBin"),
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
    target.value / "universal" / s"${name.value}-${version.value}.zip",
    target.value / s"${name.value}_${version.value}_all.deb",
    target.value / "rpm/RPMS" / packageArchitecture.value / s"${name.value}-${version.value}-1.${packageArchitecture.value}.rpm",
  ),
  ghreleaseNotes := { tagName ⇒
    SimpleReader.readLine(s"Input release notes for $tagName: ").getOrElse("")
  }
)