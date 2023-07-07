ThisBuild / version := "0.1"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "cluster-index"
  )

val jacksonVersion = "2.11.4"
lazy val akkaVersion = "2.7.0"
lazy val akkaHttpVersion = "10.2.10"

ThisBuild / libraryDependencies ++= Seq(
 "services.scalable" %% "index" % "0.29",
 "services.scalable" %% "index" % "0.29" % "protobuf",
  "com.typesafe.akka" %% "akka-http2-support" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-discovery" % akkaVersion,
  "com.typesafe.akka" %% "akka-pki" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster-tools" % akkaVersion,

  "com.typesafe.akka" %% "akka-stream-kafka" % "3.0.1",
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,

  "io.vertx" % "vertx-kafka-client" % "4.4.1"
)

enablePlugins(AkkaGrpcPlugin)
