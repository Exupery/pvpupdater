name := "pvpupdater"

version := "1.0"

scalaVersion := "2.11.12"

enablePlugins(JavaAppPackaging)

libraryDependencies += "ch.qos.logback" %  "logback-classic" % "1.2.3"
libraryDependencies += "org.postgresql" % "postgresql" % "9.4.1212"
libraryDependencies += "net.liftweb" %% "lift-json" % "3.3.0"
libraryDependencies += "org.scalaj" %% "scalaj-http" % "2.4.1"

scalacOptions ++= Seq(
  "-deprecation",
  "-feature")
