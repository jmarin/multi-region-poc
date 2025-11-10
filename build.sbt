import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport.*

// Global settings
ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "3.3.5"
ThisBuild / organization := "com.jmarin"

// Dependency versions
val PekkoVersion     = "1.1.2"
val PekkoHttpVersion = "1.1.0"
val PekkoGrpcVersion = "1.1.0"
val PekkoJdbcVersion = "1.1.1"
val AwsSdkVersion    = "2.29.16"
val PostgresVersion  = "42.7.4"
val JacksonVersion   = "2.18.2"
val ScalaTestVersion = "3.2.19"
val LogbackVersion   = "1.5.12"

// Common settings for all modules
lazy val commonSettings = Seq(
  scalacOptions ++= Seq(
    "-encoding",
    "UTF-8",
    "-deprecation",
    "-feature",
    "-unchecked",
    "-Xfatal-warnings"
  ),
  javacOptions ++= Seq(
    "-Xlint:unchecked",
    "-Xlint:deprecation"
  ),
  Test / parallelExecution := false,
  Test / fork              := true
)

// Common dependencies
lazy val commonDependencies = Seq(
  "org.apache.pekko" %% "pekko-actor-typed"         % PekkoVersion,
  "org.apache.pekko" %% "pekko-stream"              % PekkoVersion,
  "ch.qos.logback"    % "logback-classic"           % LogbackVersion,
  "org.apache.pekko" %% "pekko-actor-testkit-typed" % PekkoVersion     % Test,
  "org.scalatest"    %% "scalatest"                 % ScalaTestVersion % Test
)

// Root project
lazy val root = (project in file("."))
  .aggregate(core, persistence, protocol, grpc, api, storage, integration)
  .settings(
    name           := "multi-region-poc",
    publish / skip := true
  )

// Core domain module
lazy val core = (project in file("core"))
  .settings(commonSettings)
  .settings(
    name := "filemanager-core",
    libraryDependencies ++= commonDependencies
  )

// Persistence module (Event Sourcing + CBOR)
lazy val persistence = (project in file("persistence"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "filemanager-persistence",
    libraryDependencies ++= commonDependencies ++ Seq(
      "org.apache.pekko"                %% "pekko-persistence-typed"      % PekkoVersion,
      "org.apache.pekko"                %% "pekko-persistence-query"      % PekkoVersion,
      "org.apache.pekko"                %% "pekko-cluster-sharding-typed" % PekkoVersion,
      "org.apache.pekko"                %% "pekko-serialization-jackson"  % PekkoVersion,
      "org.apache.pekko"                %% "pekko-persistence-jdbc"       % PekkoJdbcVersion,
      "com.fasterxml.jackson.dataformat" % "jackson-dataformat-cbor"      % JacksonVersion,
      "org.postgresql"                   % "postgresql"                   % PostgresVersion,
      "org.apache.pekko"                %% "pekko-persistence-testkit"    % PekkoVersion % Test
    )
  )

// Protocol module (Protobuf definitions)
lazy val protocol = (project in file("protocol"))
  .enablePlugins(PekkoGrpcPlugin)
  .settings(commonSettings)
  .settings(
    name := "filemanager-protocol",
    libraryDependencies ++= Seq(
      "org.apache.pekko" %% "pekko-grpc-runtime" % PekkoGrpcVersion,
      "org.apache.pekko" %% "pekko-stream"       % PekkoVersion
    )
  )

// gRPC service module
lazy val grpc = (project in file("grpc"))
  .dependsOn(core, persistence, protocol, storage)
  .settings(commonSettings)
  .settings(
    name := "filemanager-grpc",
    libraryDependencies ++= commonDependencies ++ Seq(
      "org.apache.pekko" %% "pekko-grpc-runtime"           % PekkoGrpcVersion,
      "org.apache.pekko" %% "pekko-cluster-typed"          % PekkoVersion,
      "org.apache.pekko" %% "pekko-cluster-sharding-typed" % PekkoVersion,
      "org.apache.pekko" %% "pekko-discovery"              % PekkoVersion
    )
  )

// REST API module
lazy val api = (project in file("api"))
  .dependsOn(grpc)
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .settings(commonSettings)
  .settings(
    name                 := "filemanager-api",
    libraryDependencies ++= commonDependencies ++ Seq(
      "org.apache.pekko" %% "pekko-http"            % PekkoHttpVersion,
      "org.apache.pekko" %% "pekko-http-spray-json" % PekkoHttpVersion,
      "org.apache.pekko" %% "pekko-http-testkit"    % PekkoHttpVersion % Test
    ),
    Compile / mainClass  := Some("com.jmarin.filemanager.api.Main"),
    Docker / packageName := "filemanager-api",
    dockerBaseImage      := "eclipse-temurin:17-jre-jammy",
    dockerExposedPorts ++= Seq(8080, 9090, 2551),
    dockerUpdateLatest   := true
  )

// Storage module (MinIO/S3)
lazy val storage = (project in file("storage"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "filemanager-storage",
    libraryDependencies ++= commonDependencies ++ Seq(
      "software.amazon.awssdk" % "s3"               % AwsSdkVersion,
      "software.amazon.awssdk" % "netty-nio-client" % AwsSdkVersion
    )
  )

// Integration tests module
lazy val integration = (project in file("integration"))
  .dependsOn(api, grpc, storage)
  .settings(commonSettings)
  .settings(
    name           := "filemanager-integration",
    publish / skip := true,
    libraryDependencies ++= commonDependencies ++ Seq(
      "org.apache.pekko" %% "pekko-stream-testkit" % PekkoVersion     % Test,
      "org.apache.pekko" %% "pekko-http-testkit"   % PekkoHttpVersion % Test
    )
  )
