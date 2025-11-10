package com.jmarin.filemanager.api

import cats.effect.IO
import cats.syntax.all.*
import org.http4s.HttpRoutes
import sttp.tapir.server.http4s.Http4sServerInterpreter
import sttp.tapir.swagger.bundle.SwaggerInterpreter
import com.jmarin.filemanager.endpoints.FileManagerEndpoints
import com.jmarin.filemanager.storage.StorageService
import com.jmarin.filemanager.grpc.FileManagerService
import scala.concurrent.ExecutionContext

object ApiRoutes:

  def routes(
      config: ApiConfig,
      storageService: StorageService,
      grpcService: FileManagerService
  )(using ec: ExecutionContext): HttpRoutes[IO] =
    val endpointHandlers = FileManagerHandlers(config, storageService, grpcService)

    // Convert Tapir endpoints to Http4s routes
    val uploadFileRoute   = FileManagerEndpoints.uploadFile.serverLogic(endpointHandlers.uploadFile)
    val getFileInfoRoute  = FileManagerEndpoints.getFileInfo.serverLogic(endpointHandlers.getFileInfo)
    val deleteFileRoute   = FileManagerEndpoints.deleteFile.serverLogic(endpointHandlers.deleteFile)
    val listFilesRoute    = FileManagerEndpoints.listFiles.serverLogic(endpointHandlers.listFiles)
    val downloadFileRoute = FileManagerEndpoints.downloadFile.serverLogic(endpointHandlers.downloadFile)
    val healthRoute       = FileManagerEndpoints.health.serverLogic(_ => endpointHandlers.health)

    val fileRoutes = Http4sServerInterpreter[IO]().toRoutes(
      List(
        uploadFileRoute,
        getFileInfoRoute,
        deleteFileRoute,
        listFilesRoute,
        downloadFileRoute,
        healthRoute
      )
    )

    // Swagger UI routes
    val swaggerRoutes = Http4sServerInterpreter[IO]().toRoutes(
      SwaggerInterpreter()
        .fromEndpoints[IO](FileManagerEndpoints.all, "File Manager API", "1.0.0")
    )

    fileRoutes <+> swaggerRoutes
