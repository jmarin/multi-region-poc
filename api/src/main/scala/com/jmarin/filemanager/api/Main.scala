package com.jmarin.filemanager.api

import cats.effect.{IO, IOApp, Resource}
import cats.syntax.all.*
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.server.Server
import com.comcast.ip4s.*
import org.typelevel.log4cats.LoggerFactory
import org.typelevel.log4cats.slf4j.Slf4jFactory
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import com.jmarin.filemanager.storage.{StorageService, MinioStorageService}
import com.jmarin.filemanager.grpc.{FileManagerServiceImpl, FileManagerService}
import com.typesafe.config.ConfigFactory
import scala.concurrent.ExecutionContext

object Main extends IOApp.Simple:

  given LoggerFactory[IO] = Slf4jFactory.create[IO]

  def run: IO[Unit] =
    for
      config <- ApiConfig.load[IO]
      _      <- runServer(config)
    yield ()

  private def runServer(config: ApiConfig): IO[Unit] =
    // Create Pekko ActorSystem resource
    val actorSystemResource: Resource[IO, ActorSystem[Nothing]] =
      Resource.make(
        IO:
          val pekkoConfig = ConfigFactory.load()
          ActorSystem(Behaviors.empty, "file-manager-api", pekkoConfig)
      )(system => IO.blocking(system.terminate()).void)

    actorSystemResource.use: system =>
      given ExecutionContext     = system.executionContext
      given ActorSystem[Nothing] = system

      // Create StorageService
      val pekkoConfig                    = system.settings.config
      val storageService: StorageService = MinioStorageService(config.regionId, pekkoConfig)

      // Create gRPC service
      val grpcService: FileManagerService = FileManagerServiceImpl(system, storageService, config.regionId)

      // Create HTTP server
      val serverResource: Resource[IO, Server] =
        EmberServerBuilder
          .default[IO]
          .withHost(Host.fromString(config.http.host).getOrElse(host"0.0.0.0"))
          .withPort(Port.fromInt(config.http.port).getOrElse(port"8080"))
          .withHttpApp(ApiRoutes.routes(config, storageService, grpcService).orNotFound)
          .build

      serverResource.use: server =>
        IO.println(s"Server started at ${server.address}") *>
          IO.println(s"Swagger UI available at http://${config.http.host}:${config.http.port}/docs") *>
          IO.never
