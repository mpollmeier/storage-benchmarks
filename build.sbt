name := "storage-benchmarks"
ThisBuild/organization := "io.shiftleft.storage-benchmarks"

ThisBuild/scalaVersion := "2.13.2"
publish/skip := true

lazy val common = project.in(file("common"))
lazy val mvstore = project.in(file("mvstore")).dependsOn(common)

ThisBuild/resolvers ++= Seq(
  Resolver.mavenLocal,
  Resolver.bintrayRepo("shiftleft", "maven")
)

Global/cancelable := true
Global/onChangedBuildSource := ReloadOnSourceChanges
