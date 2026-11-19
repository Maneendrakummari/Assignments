ThisBuild / version := "1.0.0"
ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "spark-iceberg-sbt",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.9",
      "org.apache.iceberg" %
        "iceberg-spark-runtime-3.5_2.12" % "1.12.0"
    ),

    Compile / run / fork := true,

    // Java 17 module access for local Spark execution
    Compile / run / javaOptions ++= Seq(
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
    )
  )
