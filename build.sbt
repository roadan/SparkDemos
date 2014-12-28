name := "Demos"

version := "1.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.2.0" % "provided"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.2.0" % "provided"

libraryDependencies += "org.twitter4j" % "twitter4j-core" % "3.0.3"

libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "3.0.3"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.2.0" % "provided"

libraryDependencies += "org.apache.spark" % "spark-streaming-twitter_2.10" % "1.2.0"

scalaVersion := "2.10.4"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"