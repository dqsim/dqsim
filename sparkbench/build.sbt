ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "sparkbench",
    version := "0.1.0",
    libraryDependencies ++= Seq(
      // Spark core + SQL (marked as provided since Spark is typically supplied by the runtime)
      "org.apache.spark" %% "spark-core" % "3.5.0" % "provided",
      "org.apache.spark" %% "spark-sql"  % "3.5.0" % "provided"
    ),

    // Basic scalac options helpful for IDEs / language servers
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-encoding", "UTF-8"
    )
  )