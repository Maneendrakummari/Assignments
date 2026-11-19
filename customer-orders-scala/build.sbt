ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version      := "1.0.0"
ThisBuild / organization := "com.training"

name := "customer-orders-silver-load"

val sparkVersion = "3.5.0"

// Spark and Delta are PROVIDED: Databricks Runtime already ships compatible versions.
// Do not bundle them, or you risk conflicting jars on the cluster.
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql"   % sparkVersion % Provided,
  "io.delta"         %% "delta-spark" % "3.2.0"      % Provided
)
