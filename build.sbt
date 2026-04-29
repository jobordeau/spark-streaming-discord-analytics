ThisBuild / version      := "1.0.0"
ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "streaming.discord"

val sparkVersion  = "3.5.1"
val deltaVersion  = "3.2.0"
val kafkaVersion  = "3.4.1"

lazy val root = (project in file("."))
  .settings(
    name := "spark-streaming-discord-analytics",

    libraryDependencies ++= Seq(
      "org.apache.spark"  %% "spark-core"           % sparkVersion,
      "org.apache.spark"  %% "spark-sql"            % sparkVersion,
      "org.apache.spark"  %% "spark-streaming"      % sparkVersion,
      "org.apache.spark"  %% "spark-sql-kafka-0-10" % sparkVersion,

      "io.delta"          %% "delta-spark"          % deltaVersion,

      "org.apache.kafka"  %  "kafka-clients"        % kafkaVersion,

      "com.typesafe.play" %% "play-json"            % "2.9.4",
      "com.opencsv"       %  "opencsv"              % "5.9",
      "com.typesafe"      %  "config"               % "1.4.3",

      "org.apache.logging.log4j" % "log4j-core"       % "2.20.0",
      "org.apache.logging.log4j" % "log4j-slf4j2-impl" % "2.20.0"
    ),

    Compile / mainClass := Some("streaming.discord.DiscordStreamProcessor"),

    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _ @ _*) => MergeStrategy.concat
      case PathList("META-INF", _ @ _*)             => MergeStrategy.discard
      case "reference.conf"                         => MergeStrategy.concat
      case "application.conf"                       => MergeStrategy.concat
      case x                                        => MergeStrategy.first
    },

    assembly / assemblyJarName := s"${name.value}-${version.value}.jar",

    run / fork := true,
    run / javaOptions ++= Seq(
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
    )
  )
