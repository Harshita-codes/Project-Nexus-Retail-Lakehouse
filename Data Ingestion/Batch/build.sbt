ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version := "1.0.0"

lazy val root = (project in file("."))
  .settings(
    name := "nexus-batch",
    Compile / mainClass := Some("com.nexus.batch.ProjectNexusBatchApp"),
    assembly / mainClass := Some("com.nexus.batch.ProjectNexusBatchApp"),
    assembly / assemblyJarName := "nexus-batch.jar",
    Compile / unmanagedSources := (Compile / unmanagedSources).value
      .filterNot(_.getName == "01_bronze_ingestion.scala"),
    Test / unmanagedSources := ((Test / scalaSource).value ** "*.scala").get,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.1" % Provided,
      "io.delta" %% "delta-spark" % "3.2.0",
      "org.scalatest" %% "scalatest" % "3.2.18" % Test
    ),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _ @ _*) => MergeStrategy.concat
      case PathList("META-INF", xs @ _*)
          if xs.lastOption.exists(name => name.endsWith(".SF") || name.endsWith(".RSA") || name.endsWith(".DSA")) =>
        MergeStrategy.discard
      case "META-INF/MANIFEST.MF" => MergeStrategy.discard
      case "reference.conf" => MergeStrategy.concat
      case _ => MergeStrategy.first
    },
    Test / fork := true,
    Test / parallelExecution := false,
    Test / javaOptions ++= Seq(
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-exports=java.base/sun.util.calendar=ALL-UNNAMED"
    )
  )
