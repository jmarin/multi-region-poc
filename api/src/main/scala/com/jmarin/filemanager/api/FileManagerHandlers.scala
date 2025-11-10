package com.jmarin.filemanager.api

import cats.effect.IO
import cats.syntax.all.*
import com.jmarin.filemanager.endpoints.*
import com.jmarin.filemanager.storage.StorageService
import com.jmarin.filemanager.grpc.{
  FileManagerService,
  RegisterFileRequest,
  GetFileInfoRequest,
  DeleteFileRequest,
  ListFilesRequest as GrpcListFilesRequest,
  DownloadFileRequest
}
import org.apache.pekko.stream.scaladsl.FileIO
import java.nio.file.Files
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

class FileManagerHandlers(
    config: ApiConfig,
    storageService: StorageService,
    grpcService: FileManagerService
)(using ec: ExecutionContext):

  private def validateFile(
      fileName: String,
      contentLength: Long,
      contentType: String
  ): Option[ErrorResponse] =
    // Check file size
    if contentLength > config.validation.maxFileSize then
      Some(
        ErrorResponse(
          error = "FileTooLarge",
          message =
            s"File size ${contentLength} bytes exceeds maximum allowed size of ${config.validation.maxFileSize} bytes",
          details = Some(s"fileName: $fileName")
        )
      )
    // Check filename length
    else if fileName.length > config.validation.maxFilenameLength then
      Some(
        ErrorResponse(
          error = "FilenameTooLong",
          message = s"Filename length ${fileName.length} exceeds maximum of ${config.validation.maxFilenameLength}",
          details = Some(s"fileName: $fileName")
        )
      )
    // Check content type if validation is enabled (non-empty list)
    else if config.validation.allowedContentTypes.nonEmpty && !config.validation.allowedContentTypes.contains(
        contentType
      )
    then
      Some(
        ErrorResponse(
          error = "InvalidContentType",
          message = s"Content type '$contentType' is not allowed",
          details = Some(s"Allowed types: ${config.validation.allowedContentTypes.mkString(", ")}")
        )
      )
    else None

  def uploadFile(request: UploadFileRequest): IO[Either[ErrorResponse, UploadFileResponse]] =
    val fileId        = UUID.randomUUID().toString
    val fileName      = request.file.getName
    val contentLength = request.file.length()

    // Try to detect content type from file
    val contentType = Option(Files.probeContentType(request.file.toPath))
      .getOrElse("application/octet-stream")

    // Validate file before upload
    validateFile(fileName, contentLength, contentType) match
      case Some(error) => IO.pure(Left(error))
      case None        =>
        val uploadFlow: Future[Either[ErrorResponse, UploadFileResponse]] =
          for
            // 1. Upload file content to S3/MinIO using StorageService
            fileSource <- Future.successful(FileIO.fromPath(request.file.toPath))
            s3Key      <- storageService.uploadFile(
                            fileId = fileId,
                            fileName = fileName,
                            content = fileSource,
                            contentLength = contentLength,
                            contentType = contentType
                          )

            // 2. Register file metadata with Pekko backend via gRPC
            grpcRequest = RegisterFileRequest(
                            fileId = fileId,
                            name = fileName,
                            s3Key = s3Key,
                            sizeBytes = contentLength,
                            mimeType = contentType,
                            owner = request.owner,
                            allowedRegions = Seq.empty, // Unrestricted
                            region = config.regionId
                          )

            grpcResponse <- grpcService.registerFile(grpcRequest)
          yield
            if grpcResponse.status.isSuccess then
              Right(
                UploadFileResponse(
                  fileId = fileId,
                  fileName = fileName,
                  contentType = contentType,
                  contentLength = contentLength,
                  s3Key = s3Key,
                  status = "uploaded",
                  message = s"File $fileName uploaded successfully to ${config.regionId}"
                )
              )
            else
              Left(
                ErrorResponse(
                  error = "RegistrationFailed",
                  message = s"File uploaded but registration failed: ${grpcResponse.errorMessage}",
                  details = Some(s"s3Key: $s3Key")
                )
              )

        IO.fromFuture(IO(uploadFlow))
          .recover:
            case ex: Throwable =>
              Left(
                ErrorResponse(
                  error = "UploadFailed",
                  message = s"Failed to upload file: ${ex.getMessage}",
                  details = Some(ex.getClass.getName)
                )
              )

  def getFileInfo(fileId: String): IO[Either[ErrorResponse, FileInfoResponse]] =
    val request = GetFileInfoRequest(fileId = fileId)

    val getInfoFlow: Future[Either[ErrorResponse, FileInfoResponse]] =
      grpcService
        .getFileInfo(request)
        .map: response =>
          if response.found && response.metadata.isDefined then
            val metadata = response.metadata.get
            Right(
              FileInfoResponse(
                fileId = metadata.fileId,
                fileName = metadata.name,
                contentType = metadata.mimeType,
                contentLength = metadata.sizeBytes,
                owner = metadata.owner,
                createdAt = java.time.Instant.ofEpochMilli(metadata.createdAt).toString,
                replicas = metadata.replicas
                  .map: replica =>
                    ReplicaInfo(
                      regionId = replica.regionId,
                      s3Key = replica.s3Key,
                      status = "active" // For now, all replicas are active
                    )
                  .toList
              )
            )
          else
            Left(
              ErrorResponse(
                error = "NotFound",
                message = s"File not found: $fileId",
                details = None
              )
            )

    IO.fromFuture(IO(getInfoFlow))
      .recover:
        case ex: Throwable =>
          Left(
            ErrorResponse(
              error = "GetInfoFailed",
              message = s"Failed to get file info: ${ex.getMessage}",
              details = Some(ex.getClass.getName)
            )
          )

  def deleteFile(fileId: String): IO[Either[ErrorResponse, DeleteFileResponse]] =
    val request = DeleteFileRequest(fileId = fileId)

    val deleteFlow: Future[Either[ErrorResponse, DeleteFileResponse]] =
      grpcService
        .deleteFile(request)
        .map: response =>
          if response.success then
            Right(
              DeleteFileResponse(
                fileId = fileId,
                status = "deleted",
                message = s"File $fileId deleted successfully"
              )
            )
          else
            Left(
              ErrorResponse(
                error = "DeleteFailed",
                message = response.errorMessage,
                details = None
              )
            )

    IO.fromFuture(IO(deleteFlow))
      .recover:
        case ex: Throwable =>
          Left(
            ErrorResponse(
              error = "DeleteFailed",
              message = s"Failed to delete file: ${ex.getMessage}",
              details = Some(ex.getClass.getName)
            )
          )

  def listFiles(request: ListFilesRequest): IO[Either[ErrorResponse, ListFilesResponse]] =
    val grpcRequest = GrpcListFilesRequest(
      owner = request.owner,
      page = request.cursor.flatMap(_.toIntOption).getOrElse(0),
      pageSize = request.limit.getOrElse(100)
    )

    val listFlow: Future[Either[ErrorResponse, ListFilesResponse]] =
      grpcService
        .listFiles(grpcRequest)
        .map: response =>
          Right(
            ListFilesResponse(
              files = response.files
                .map: fileInfo =>
                  FileInfoResponse(
                    fileId = fileInfo.fileId,
                    fileName = fileInfo.name,
                    contentType = fileInfo.mimeType,
                    contentLength = fileInfo.sizeBytes,
                    owner = fileInfo.owner,
                    createdAt = java.time.Instant.ofEpochMilli(fileInfo.createdAt).toString,
                    replicas = fileInfo.replicas
                      .map: replica =>
                        ReplicaInfo(
                          regionId = replica.regionId,
                          s3Key = replica.s3Key,
                          status = "active"
                        )
                      .toList
                  )
                .toList,
              totalCount = response.totalCount,
              nextCursor = None // Pagination not fully implemented yet
            )
          )

    IO.fromFuture(IO(listFlow))
      .recover:
        case ex: Throwable =>
          Left(
            ErrorResponse(
              error = "ListFailed",
              message = s"Failed to list files: ${ex.getMessage}",
              details = Some(ex.getClass.getName)
            )
          )

  def downloadFile(fileId: String): IO[Either[ErrorResponse, DownloadFileResponse]] =
    val request = DownloadFileRequest(
      fileId = fileId,
      region = config.regionId
    )

    val downloadFlow: Future[Either[ErrorResponse, DownloadFileResponse]] =
      grpcService
        .downloadFile(request)
        .map: response =>
          if response.status.isSuccess then
            val expiresAt = java.time.Instant.now.plusSeconds(3600).toString // 1 hour from now
            Right(
              DownloadFileResponse(
                fileId = fileId,
                downloadUrl = response.presignedUrl,
                expiresAt = expiresAt
              )
            )
          else if response.status.isNotFound then
            Left(
              ErrorResponse(
                error = "NotFound",
                message = s"File not found: $fileId",
                details = None
              )
            )
          else
            Left(
              ErrorResponse(
                error = "DownloadFailed",
                message = response.errorMessage,
                details = None
              )
            )

    IO.fromFuture(IO(downloadFlow))
      .recover:
        case ex: Throwable =>
          Left(
            ErrorResponse(
              error = "DownloadFailed",
              message = s"Failed to generate download URL: ${ex.getMessage}",
              details = Some(ex.getClass.getName)
            )
          )

  def health: IO[Either[ErrorResponse, HealthResponse]] =
    IO.pure(
      Right(
        HealthResponse(
          status = "OK",
          version = "1.0.0",
          uptime = System.currentTimeMillis() // Simplified - ideally track from app start
        )
      )
    )

object FileManagerHandlers:
  def apply(config: ApiConfig, storageService: StorageService, grpcService: FileManagerService)(using
      ec: ExecutionContext
  ): FileManagerHandlers =
    new FileManagerHandlers(config, storageService, grpcService)
