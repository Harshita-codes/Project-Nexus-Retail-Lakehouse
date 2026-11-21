ThisBuild / scalaVersion := "2.13.16"

lazy val root = (project in file("."))
  .settings(
    name := "nexus-kafka-producer",
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-clients" % "3.9.1",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.18.2"
    ),
    assembly / assemblyJarName := "nexus-kafka-producer.jar",
    assembly / mainClass := Some("com.nexus.producer.OrderEventProducer"),
    assembly / assemblyMergeStrategy := {
      case path if path.endsWith("module-info.class") => MergeStrategy.discard
      case path if path.contains("META-INF/versions/") => MergeStrategy.first
      case path if path.startsWith("META-INF/") => MergeStrategy.discard
      case path if path.endsWith(".properties") => MergeStrategy.filterDistinctLines
      case path => (assembly / assemblyMergeStrategy).value(path)
    },
    Compile / run / fork := true
  )
