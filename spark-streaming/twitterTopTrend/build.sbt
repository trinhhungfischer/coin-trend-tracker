

name := "twitterTopTrend"
version := "1.0"
scalaVersion := "2.12.15"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.3.1" % "provided"

libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.32.0"
libraryDependencies += "joda-time" % "joda-time" % "2.7" withSources()
libraryDependencies += "org.joda" % "joda-convert" % "1.2" withSources()

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "3.2.0"