ThisBuild / version := "0.1"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "cluster-index"
  )

val jacksonVersion = "2.11.4"
lazy val akkaVersion = "2.6.14"
lazy val akkaHttpVersion = "10.2.3"

ThisBuild / libraryDependencies ++= Seq(
 "services.scalable" %% "index" % "0.19",
 "services.scalable" %% "index" % "0.19" % "protobuf",
  "com.typesafe.akka" %% "akka-http2-support" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-discovery" % akkaVersion,
  "com.typesafe.akka" %% "akka-pki" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster-tools" % akkaVersion,

  "com.typesafe.akka" %% "akka-stream-kafka" % "2.1.0",
  "com.typesafe.akka" %% "akka-stream" % akkaVersion
)

enablePlugins(AkkaGrpcPlugin)
