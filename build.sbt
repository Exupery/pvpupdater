name := "pvpupdater"

version := "1.0"

enablePlugins(JavaAppPackaging)

libraryDependencies += "ch.qos.logback" %  "logback-classic" % "1.2.3"
libraryDependencies += "org.postgresql" % "postgresql" % "9.4.1212"
libraryDependencies += "net.liftweb" %% "lift-json" % "3.3.0"

scalacOptions ++= Seq(
  "-deprecation",
  "-feature")
