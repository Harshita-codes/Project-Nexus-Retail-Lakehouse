ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "project-nexus-batch-tests",
    Compile / unmanagedSources := Seq(baseDirectory.value.getParentFile / "00_batch_helpers.scala"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.9",
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    ),
    Test / fork := true,
    Test / parallelExecution := false,
    Test / javaOptions ++= Seq(
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED"
    )
  )
