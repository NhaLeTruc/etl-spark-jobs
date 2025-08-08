name := "spark-app-template"
homepage := Some(url("https://github.vn.boblee.com/etl/etl-spark-jobs"))
description := "kyc-sc"
organization := "au.com.nab.kyc-sc" // replace etl with your team name

version := sys.env.get("ARTEFACT_VERSION").getOrElse("0.1-SNAPSHOT")

scalaVersion := "2.11.12"
val sparkVersion = "2.4.0"

initialCommands in console :=
  s"""
     |val sc = new org.apache.spark.SparkContext("local", "shell")
     |val sqlContext = new org.apache.spark.sql.SQLContext(sc)
     |import sqlContext.implicits._
     |import org.apache.spark.sql.functions._
     |""".stripMargin

publishTo := Some("repository Realm" at "https://private.repository.vn/repository/" + sys.env.get("REPO_REPO")
  .getOrElse("DEPUTY-PILOT-FCMP-SBT-build"))

// Publish assembly fat jar
artifact in(Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.withClassifier(Some("assembly"))
}
addArtifact(artifact in(Compile, assembly), assembly)

credentials += Credentials(
  "repository Realm",
  "private.repository.vn",
  sys.env.get("REPO_CREDS_USR").getOrElse(""),
  sys.env.get("REPO_CREDS_PSW").getOrElse("")
)

resolvers += "repository" at "https://private.repository.vn/repository/FCMP-MAVEN-build"

resolvers += "CDH" at "https://private.repository.vn/repository/cloudera-remote"

resolvers += "MavenRepo1" at "https://private.repository.vn/repository/maven-repo1"

val excludeHBinding = ExclusionRule(organization = "com.holdenkarau")

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
// libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.0-cdh6.2.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.8" % "provided"
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.17.1"

// https://mvnrepository.com/artifact/com.oracle.jdbc/ojdbc8
// libraryDependencies += "com.oracle.ojdbc" % "ojdbc8" % "19.3.0.0"
libraryDependencies += "com.oracle.databse.jdbc" % "ojdbc8" % "21.1.0.0"

// https://mvnrepository.com/artifact/org.xerial.snappy/snappy-java
libraryDependencies += "org.xerial.snappy" % "snappy-java" % "1.1.4"

// https://mvnrepository.com/aritfact/org.scalatest/scalatest
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.8"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % "test, it"
libraryDependencies += "org.pegdown" % "pegdown" % "1.1.0" % "test, it"

// https://github.com/holdenk/spark-testing-base
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.3.0_0.12.0" % "test, it"

// https://pureconfig.github.io/
libraryDependencies += "com.github.pureconfig" %% "pureconfig" % "0.12.1"
libraryDependencies += "com.typesafe" % "config" % "1.3.4"

// https://github.com.scopt/scopt
libraryDependencies += "com.github.scopt" %% "scopt" % "3.7.1"

// https://github.com/lightbend/scala-logging
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
libraryDependencies += "com.databrick" %% "spark-xml" % "0.12.0"

// https://github.vn.boblee.com/etl/sparkbase
libraryDependencies += "au.com.nab.etl" % "sparkbase" % "2.22.0-42-4e555de" excludeAll(excludeHBinding)

// Creating Scla Fat Jars for Spark on SBT
// Source: https://stackoverflow.com/questions/23280494/sbt-assembly-error-deduplicate-different-file-contents-found-in-the-following
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

// test run settings
parallelExecution in Test := false
fork in Test := true
fork in IntegrationTest := true
assembly /test := {}

// Enable integration tests
Defaults.itSettings
lazy val FunTest = config("it") extend (Test)
lazy val root = project.in(file(".")).configs(FunTest)

// Measure time for each test
Test / testOptions += Tests.Argument("-oD")
IntegrationTest / testOptions += Tests.Argument("-oD")

// Generate test results in HTML format
Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-h", "target/test-reports")
Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-Y", "test-report/fixes/styles.css")
IntegrationTest / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-h", "target/test-reports")
IntegrationTest / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-Y", "test-report/fixes/styles.css")