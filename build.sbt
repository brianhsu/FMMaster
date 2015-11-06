name := "FMMaster"

organization := "moe.brianhsu"

version := "0.0.1"

scalaVersion := "2.11.7"

scalacOptions ++= Seq("-deprecation")

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.3.14",
  "commons-codec" % "commons-codec" % "1.10",
  "org.squeryl" %% "squeryl" % "0.9.6-RC4",
  "org.xerial" % "sqlite-jdbc" % "3.8.11.2",
  "org.apache.commons" % "commons-dbcp2" % "2.1.1",
  "net.sf.jpathwatch" % "jpathwatch" % "0.95"

)

fork := true

