ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "kafkastreamingicebergdemo",
    libraryDependencies ++= Seq(
      // Apache Spark SQL
      "org.apache.spark" %% "spark-sql" % "3.5.9",
      // Kafka connector for Spark Structured Streaming
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.9",
      // Apache Iceberg runtime
      "org.apache.iceberg" % "iceberg-spark-runtime-3.5_2.12" % "1.12.0",
      // Delta Lake runtime for durable copies of streaming tables
      "io.delta" %% "delta-spark" % "3.3.2",
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    ),
    Compile / run / fork := true,
    Compile / run / javaOptions ++= Seq(
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED"
    ),
    Test / fork := true,
    Test / parallelExecution := false,
    Test / javaOptions ++= Seq(
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED"
    )
  )