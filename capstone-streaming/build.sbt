
ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "capstone-streaming",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.0",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.0"
    )
  )
