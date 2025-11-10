package com.jmarin.filemanager.grpc

import com.jmarin.filemanager.domain.{Command, FileMetadata as DomainFileMetadata}
import com.jmarin.filemanager.persistence.FileManager as FileManagerActor
import com.jmarin.filemanager.storage.StorageService
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, EntityRef}
import org.apache.pekko.util.Timeout
import org.apache.pekko.actor.typed.scaladsl.AskPattern.Askable
import org.slf4j.LoggerFactory

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

/** Implementation of the FileManagerService gRPC service.
  *
  * This service provides the gRPC API for file management operations, integrating with the FileManager actor (event
  * sourced) and MinIO storage service.
  */
class FileManagerServiceImpl(
    system: ActorSystem[?],
    storageService: StorageService,
    regionId: String
) extends FileManagerService:

  private val logger             = LoggerFactory.getLogger(getClass)
  private given ActorSystem[?]   = system
  private given ExecutionContext = system.executionContext
  private given Timeout          = Timeout(10.seconds)

  private val sharding: ClusterSharding = ClusterSharding(system)

  /** Register a file that has already been uploaded to storage. */
  override def registerFile(request: RegisterFileRequest): Future[RegisterFileResponse] =
    logger.info(s"gRPC registerFile: fileId=${request.fileId}, name=${request.name}")

    val entityRef: EntityRef[Command] = sharding.entityRefFor(FileManagerActor.TypeKey, request.fileId)

    val uploadCommand = Command.UploadFile(
      fileId = request.fileId,
      fileName = request.name,
      fileSize = request.sizeBytes,
      contentType = request.mimeType,
      checksum = "", // Checksum not provided in protobuf, would need to be computed
      region = request.region,
      replyTo = null // Will be set by ask pattern
    )

    entityRef
      .ask[Command.UploadResponse](replyTo => uploadCommand.copy(replyTo = replyTo))
      .map:
        case Command.UploadResponse.Success(metadata) =>
          RegisterFileResponse(
            fileId = request.fileId,
            metadata = Some(convertToGrpcMetadata(metadata)),
            status = ResponseStatus.SUCCESS,
            errorMessage = ""
          )
        case Command.UploadResponse.Failure(reason)   =>
          RegisterFileResponse(
            fileId = request.fileId,
            metadata = None,
            status = ResponseStatus.INTERNAL_ERROR,
            errorMessage = reason
          )
      .recover: ex =>
        logger.error(s"Failed to register file: fileId=${request.fileId}", ex)
        RegisterFileResponse(
          fileId = request.fileId,
          metadata = None,
          status = ResponseStatus.INTERNAL_ERROR,
          errorMessage = ex.getMessage
        )

  /** Get file metadata. */
  override def getFileInfo(request: GetFileInfoRequest): Future[GetFileInfoResponse] =
    logger.info(s"gRPC getFileInfo: fileId=${request.fileId}")

    val entityRef: EntityRef[Command] = sharding.entityRefFor(FileManagerActor.TypeKey, request.fileId)

    entityRef
      .ask[Command.FileInfoResponse](replyTo => Command.GetFileInfo(request.fileId, replyTo))
      .map:
        case Command.FileInfoResponse.Success(metadata) =>
          GetFileInfoResponse(
            metadata = Some(convertToGrpcMetadata(metadata)),
            found = true
          )
        case Command.FileInfoResponse.NotFound(_)       =>
          GetFileInfoResponse(
            metadata = None,
            found = false
          )
        case Command.FileInfoResponse.Failure(reason)   =>
          logger.error(s"Failed to get file info: ${reason}")
          GetFileInfoResponse(metadata = None, found = false)
      .recover: ex =>
        logger.error(s"Failed to get file info: fileId=${request.fileId}", ex)
        GetFileInfoResponse(metadata = None, found = false)

  /** Delete a file. */
  override def deleteFile(request: DeleteFileRequest): Future[DeleteFileResponse] =
    logger.info(s"gRPC deleteFile: fileId=${request.fileId}")

    val entityRef: EntityRef[Command] = sharding.entityRefFor(FileManagerActor.TypeKey, request.fileId)

    // First get file info to obtain S3 key
    entityRef
      .ask[Command.FileInfoResponse](replyTo => Command.GetFileInfo(request.fileId, replyTo))
      .flatMap:
        case Command.FileInfoResponse.Success(metadata) =>
          // Delete the file
          entityRef
            .ask[Command.DeleteResponse](replyTo => Command.DeleteFile(request.fileId, replyTo))
            .flatMap:
              case Command.DeleteResponse.Success(_)      =>
                // Also delete from storage - construct s3Key from region and fileId
                val s3Key = s"${regionId}/${metadata.fileId}"
                storageService
                  .deleteFile(s3Key)
                  .map: _ =>
                    DeleteFileResponse(success = true, errorMessage = "")
                  .recover: ex =>
                    logger.warn(s"Deleted from actor but failed to delete from storage: ${ex.getMessage}")
                    DeleteFileResponse(success = true, errorMessage = "")
              case Command.DeleteResponse.NotFound(_)     =>
                Future.successful(DeleteFileResponse(success = false, errorMessage = "File not found"))
              case Command.DeleteResponse.Failure(reason) =>
                Future.successful(DeleteFileResponse(success = false, errorMessage = reason))
        case Command.FileInfoResponse.NotFound(_)       =>
          Future.successful(DeleteFileResponse(success = false, errorMessage = "File not found"))
        case Command.FileInfoResponse.Failure(reason)   =>
          Future.successful(DeleteFileResponse(success = false, errorMessage = reason))
      .recover: ex =>
        logger.error(s"Failed to delete file: fileId=${request.fileId}", ex)
        DeleteFileResponse(success = false, errorMessage = ex.getMessage)

  /** List files (simplified - requires read-side projection in production). */
  override def listFiles(request: ListFilesRequest): Future[ListFilesResponse] =
    logger.info(s"gRPC listFiles: owner=${request.owner}")

    // In production, this would query a read-side projection (database)
    Future.successful(
      ListFilesResponse(
        files = Seq.empty,
        totalCount = 0
      )
    )

  /** Download a file by generating a presigned URL. */
  override def downloadFile(request: DownloadFileRequest): Future[DownloadFileResponse] =
    logger.info(s"gRPC downloadFile: fileId=${request.fileId}")

    val entityRef: EntityRef[Command] = sharding.entityRefFor(FileManagerActor.TypeKey, request.fileId)

    entityRef
      .ask[Command.DownloadResponse](replyTo => Command.DownloadFile(request.fileId, replyTo))
      .flatMap:
        case Command.DownloadResponse.Success(metadata) =>
          // Construct s3Key from region and fileId
          val s3Key = s"${regionId}/${metadata.fileId}"
          storageService
            .generatePresignedUrl(s3Key, 1.hour)
            .map: url =>
              DownloadFileResponse(
                presignedUrl = url.toString,
                s3Key = s3Key,
                status = ResponseStatus.SUCCESS,
                errorMessage = ""
              )
            .recover: ex =>
              logger.error(s"Failed to generate presigned URL: ${ex.getMessage}")
              DownloadFileResponse(
                presignedUrl = "",
                s3Key = "",
                status = ResponseStatus.INTERNAL_ERROR,
                errorMessage = ex.getMessage
              )
        case Command.DownloadResponse.NotFound(fileId)  =>
          Future.successful(
            DownloadFileResponse(
              presignedUrl = "",
              s3Key = "",
              status = ResponseStatus.NOT_FOUND,
              errorMessage = s"File not found: $fileId"
            )
          )
        case Command.DownloadResponse.Failure(reason)   =>
          Future.successful(
            DownloadFileResponse(
              presignedUrl = "",
              s3Key = "",
              status = ResponseStatus.INTERNAL_ERROR,
              errorMessage = reason
            )
          )
      .recover: ex =>
        logger.error(s"Failed to download file: fileId=${request.fileId}", ex)
        DownloadFileResponse(
          presignedUrl = "",
          s3Key = "",
          status = ResponseStatus.INTERNAL_ERROR,
          errorMessage = ex.getMessage
        )

  /** Convert domain FileMetadata to gRPC FileMetadata. */
  private def convertToGrpcMetadata(metadata: DomainFileMetadata): FileMetadata =
    FileMetadata(
      fileId = metadata.fileId,
      name = metadata.fileName,
      sizeBytes = metadata.fileSize,
      mimeType = metadata.contentType,
      s3Key = s"${regionId}/${metadata.fileId}", // Construct S3 key from region and fileId
      owner = "",                                // Not tracked in current domain model
      createdAt = metadata.uploadedAt.toEpochMilli,
      modifiedAt = metadata.uploadedAt.toEpochMilli,
      status = "active",
      allowedRegions = Seq.empty,                // Not tracked in current domain model
      uploadedRegion = regionId,
      replicas = metadata.replicas
        .map: regionId =>
          // Convert region string to ReplicaInfo
          ReplicaInfo(
            regionId = regionId,
            s3Key = s"${regionId}/${metadata.fileId}",
            replicatedAt = metadata.uploadedAt.toEpochMilli // Use uploadedAt as approximation
          )
        .toSeq
    )
