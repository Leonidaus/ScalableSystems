scalaVersion := "2.13.16"

name    := "scalable-systems"
version := "0.1.0"

val flinkVersion = "2.3.0"
val log4jVersion = "2.25.3" // matches the log4j shipped in the flink:2.3.0 image

libraryDependencies ++= Seq(
  // Provided: kept out of the fat jar, but on the local run classpath so MiniCluster runs work
  "org.apache.flink" % "flink-streaming-java" % flinkVersion % Provided,
  "org.apache.flink" % "flink-clients"       % flinkVersion % Provided,
  "org.apache.flink" % "flink-connector-files" % flinkVersion % Provided,
  "org.apache.flink" % "flink-test-utils"    % flinkVersion % Test,
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % log4jVersion % Provided,
  "org.apache.logging.log4j" % "log4j-core"       % log4jVersion % Provided,
  "org.apache.logging.log4j" % "log4j-api"        % log4jVersion % Provided
)

Compile / run := Defaults
  .runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner)
  .evaluated

Compile / runMain := Defaults
  .runMainTask(Compile / fullClasspath, Compile / runMain / runner)
  .evaluated

fork := true

// Java 17 needs these opened explicitly.
javaOptions ++= Seq(
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
)

// Two main classes exist, so `run` must be pinned or sbt prompts for a choice.
Compile / run / mainClass  := Some("tick.FlinkSmokeJob")
assembly / mainClass       := Some("tick.FlinkSmokeJob")
assembly / assemblyJarName := s"${name.value}-${version.value}.jar"

assembly / assemblyMergeStrategy := {
  // Connector discovery relies on these being concatenated, not dropped.
  case PathList("META-INF", "services", _*) => MergeStrategy.concat
  case PathList("META-INF", _*)             => MergeStrategy.discard
  case "reference.conf"                     => MergeStrategy.concat
  // Local-only logging config; the cluster supplies its own.
  case "log4j2.properties"                  => MergeStrategy.discard
  case _                                    => MergeStrategy.first
}
