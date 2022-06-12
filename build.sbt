name := "cluster-index"

version := "0.1"

scalaVersion := "2.13.6"

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots")
)

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.3" % Test,
  "ch.qos.logback" % "logback-classic" % "1.2.3",

  "com.google.guava" % "guava" % "27.1-jre",
  "org.apache.commons" % "commons-lang3" % "3.8.1",

  "services.scalable" %% "index" % "0.12",
  "org.apache.commons" % "commons-compress" % "1.21"
)

//javaOptions := Seq("-Xdebug", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005")

enablePlugins(AkkaGrpcPlugin)