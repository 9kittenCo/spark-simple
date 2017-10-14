name := "simple-spark"

version := "0.1"

scalaVersion := "2.11.11"

//scalacOptions ++= Seq("-deprecation")

val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
  "io.netty" % "netty" % "3.6.2.Final",
//  "org.slf4j" % "slf4j-nop" % "1.7.25",
//  "org.scala-lang" %% "scala-compiler" % "2.11.11",
//  "jline" %% "jline" % "2.12.1" % Provided,
//  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4",
  "org.scala-lang" % "scala-reflect" % "2.11.11",
  "org.scala-lang.modules" %% "scala-xml" % "1.0.4",
  "org.apache.spark" %% "spark-core" % sparkVersion ,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/")

fork in run := true

//// set the main class for packaging the main jar
//mainClass in(Compile, packageBin) := Some("exercises.Exercise1")
//
//// set the main class for the main 'sbt run' task
//mainClass in(Compile, run) := Some("exercises.Exercise1")