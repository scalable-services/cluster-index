ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "cluster-index"
  )

val indexVersion = "0.34"

ThisBuild / libraryDependencies ++= Seq(
  "org.scalactic" %% "scalactic" % "3.1.0",
  "org.scalatest" %% "scalatest" % "3.1.0" % "test",
  "services.scalable" %% "index" % indexVersion,
  "services.scalable" %% "index" % indexVersion % "protobuf"
)

ThisBuild / libraryDependencies += "com.datastax.oss" % "java-driver-core" % "4.17.0"

enablePlugins(AkkaGrpcPlugin)
