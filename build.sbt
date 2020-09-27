name := "qaFrameWork"

version := "1.0.11"

scalaVersion := "2.11.12"

val spark_kinesis_version = "2.2.0"

val sparkVersion = "2.4.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
)
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion  % "provided"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
//libraryDependencies += "MrPowers" % "spark-fast-tests" % "0.21.1-s_2.12" % "test"
//libraryDependencies += "mrpowers" % "spark-daria" % "0.37.1-s_2.12"
libraryDependencies += "com.typesafe" % "config" % "1.2.1"

// test suite settings
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")
//javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
// Show runtime of tests
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")
envVars in Test := Map("PROJECT_ENV" -> "test")
