resolvers += "clojars.org" at "http://clojars.org/repo"

libraryDependencies ++= Seq("net.debasishg" %% "redisclient" % "3.0")
libraryDependencies ++= Seq("net.liftweb" %% "lift-json" % "2.6")
libraryDependencies += "com.typesafe" % "config" % "1.2.1"
libraryDependencies += "clj-time" % "clj-time" % "0.4.1"
libraryDependencies += "org.scalaj" %% "scalaj-http" % "1.1.5"
libraryDependencies += "org.apache.storm" % "storm-core" % "0.9.3" % "provided"
libraryDependencies += "org.apache.storm" % "storm-kafka" % "0.9.3" % "provided"
libraryDependencies += "org.apache.kafka" %% "kafka" % "0.8.2.1" % "provided"
lazy val root = (project in file(".")).
	settings(
			name := "collect_push",
			version := "1.0",
			scalaVersion := "2.11.7"
	) 
