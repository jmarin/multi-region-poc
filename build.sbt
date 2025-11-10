import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport.*

// Global settings
ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "3.3.5"
ThisBuild / organization := "com.jmarin"

// Dependency versions
val PekkoVersion          = "1.1.3"
val PekkoHttpVersion      = "1.1.0"
val PekkoGrpcVersion      = "1.1.0"
val PekkoJdbcVersion      = "1.1.1"
val AwsSdkVersion         = "2.29.16"
val PostgresVersion       = "42.7.4"
val FlywayVersion         = "10.21.0"
val JacksonVersion        = "2.17.2"
val ScalaTestVersion      = "3.2.19"
val LogbackVersion        = "1.5.12"
val TapirVersion          = "1.11.10"
val Http4sVersion         = "0.23.30"
val CatsEffectVersion     = "3.5.7"
val CirceVersion          = "0.14.10"
val Log4CatsVersion       = "2.7.0"
val TestContainersVersion = "0.41.4"

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
  Test / fork              := true,
  Test / javaOptions ++= Seq(
    "-Dio.netty.tryReflectionSetAccessible=true"
  )
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
  .aggregate(core, persistence, protocol, grpc, endpoints, api, storage, integration)
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
      "org.flywaydb"                     % "flyway-core"                  % FlywayVersion,
      "org.flywaydb"                     % "flyway-database-postgresql"   % FlywayVersion,
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
  .enablePlugins(PekkoGrpcPlugin)
  .dependsOn(core, persistence, storage)
  .settings(commonSettings)
  .settings(
    name := "grpc",
    libraryDependencies ++= commonDependencies ++ Seq(
      "org.apache.pekko" %% "pekko-grpc-runtime"           % PekkoGrpcVersion,
      "org.apache.pekko" %% "pekko-http"                   % PekkoHttpVersion,
      "org.apache.pekko" %% "pekko-cluster-typed"          % PekkoVersion,
      "org.apache.pekko" %% "pekko-cluster-sharding-typed" % PekkoVersion,
      "org.apache.pekko" %% "pekko-discovery"              % PekkoVersion
    )
  )

// Endpoints module (Tapir endpoint definitions)
lazy val endpoints = (project in file("endpoints"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "filemanager-endpoints",
    libraryDependencies ++= Seq(
      "com.softwaremill.sttp.tapir" %% "tapir-core"       % TapirVersion,
      "com.softwaremill.sttp.tapir" %% "tapir-json-circe" % TapirVersion,
      "io.circe"                    %% "circe-core"       % CirceVersion,
      "io.circe"                    %% "circe-generic"    % CirceVersion,
      "io.circe"                    %% "circe-parser"     % CirceVersion,
      "org.scalatest"               %% "scalatest"        % ScalaTestVersion % Test
    )
  )

// REST API module (Http4s + Cats Effect implementation)
lazy val api = (project in file("api"))
  .dependsOn(endpoints, grpc)
  .enablePlugins(JavaAppPackaging, DockerPlugin)
  .settings(commonSettings)
  .settings(
    name                 := "filemanager-api",
    libraryDependencies ++= Seq(
      "org.typelevel"               %% "cats-effect"             % CatsEffectVersion,
      "org.http4s"                  %% "http4s-ember-server"     % Http4sVersion,
      "org.http4s"                  %% "http4s-dsl"              % Http4sVersion,
      "org.http4s"                  %% "http4s-circe"            % Http4sVersion,
      "com.softwaremill.sttp.tapir" %% "tapir-http4s-server"     % TapirVersion,
      "com.softwaremill.sttp.tapir" %% "tapir-swagger-ui-bundle" % TapirVersion,
      "org.typelevel"               %% "log4cats-slf4j"          % "2.7.0",
      "ch.qos.logback"               % "logback-classic"         % LogbackVersion,
      "org.apache.pekko"            %% "pekko-actor-typed"       % PekkoVersion,
      "org.apache.pekko"            %% "pekko-stream"            % PekkoVersion,
      "org.scalatest"               %% "scalatest"               % ScalaTestVersion % Test,
      "org.http4s"                  %% "http4s-ember-client"     % Http4sVersion    % Test
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
  .dependsOn(core, api, grpc, storage, persistence)
  .settings(commonSettings)
  .settings(
    name           := "filemanager-integration",
    publish / skip := true,
    Test / scalacOptions += "-Wconf:cat=deprecation:s",
    libraryDependencies ++= commonDependencies ++ Seq(
      "org.apache.pekko" %% "pekko-stream-testkit"            % PekkoVersion          % Test,
      "org.apache.pekko" %% "pekko-http-testkit"              % PekkoHttpVersion      % Test,
      "com.dimafeng"     %% "testcontainers-scala-scalatest"  % TestContainersVersion % Test,
      "com.dimafeng"     %% "testcontainers-scala-postgresql" % TestContainersVersion % Test,
      "com.dimafeng"     %% "testcontainers-scala-minio"      % TestContainersVersion % Test,
      "org.http4s"       %% "http4s-ember-client"             % Http4sVersion         % Test,
      "org.typelevel"    %% "log4cats-slf4j"                  % Log4CatsVersion       % Test
    )
  )
