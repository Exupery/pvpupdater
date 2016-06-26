name := "pvpupdater"

version := "1.0"

enablePlugins(JavaAppPackaging)

libraryDependencies += "ch.qos.logback" %  "logback-classic" % "1.1.7"
libraryDependencies += "org.postgresql" % "postgresql" % "9.4.1208.jre7"
libraryDependencies += "net.liftweb" % "lift-json_2.10" % "2.6.3"
