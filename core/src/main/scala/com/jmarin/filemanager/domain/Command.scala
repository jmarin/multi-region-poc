package com.jmarin.filemanager.domain

import org.apache.pekko.actor.typed.ActorRef

/** Commands for the FileManager actor */
sealed trait Command extends CborSerializable

object Command:
  /** Upload a file to the system
    *
    * @param fileId
    *   Unique identifier for the file
    * @param fileName
    *   Original file name
    * @param fileSize
    *   Size in bytes
    * @param contentType
    *   MIME type
    * @param checksum
    *   SHA-256 checksum
    * @param region
    *   Region where file is being uploaded
    * @param replyTo
    *   Actor to send the response
    */
  final case class UploadFile(
      fileId: String,
      fileName: String,
      fileSize: Long,
      contentType: String,
      checksum: String,
      region: String,
      replyTo: ActorRef[UploadResponse]
  ) extends Command

  /** Download a file from the system
    *
    * @param fileId
    *   Unique identifier for the file
    * @param replyTo
    *   Actor to send the response
    */
  final case class DownloadFile(
      fileId: String,
      replyTo: ActorRef[DownloadResponse]
  ) extends Command

  /** Delete a file from the system
    *
    * @param fileId
    *   Unique identifier for the file
    * @param replyTo
    *   Actor to send the response
    */
  final case class DeleteFile(
      fileId: String,
      replyTo: ActorRef[DeleteResponse]
  ) extends Command

  /** Get file metadata information
    *
    * @param fileId
    *   Unique identifier for the file
    * @param replyTo
    *   Actor to send the response
    */
  final case class GetFileInfo(
      fileId: String,
      replyTo: ActorRef[FileInfoResponse]
  ) extends Command

  // Response types
  sealed trait Response extends CborSerializable

  sealed trait UploadResponse extends Response
  object UploadResponse:
    final case class Success(metadata: FileMetadata) extends UploadResponse
    final case class Failure(reason: String)         extends UploadResponse

  sealed trait DownloadResponse extends Response
  object DownloadResponse:
    final case class Success(metadata: FileMetadata) extends DownloadResponse
    final case class NotFound(fileId: String)        extends DownloadResponse
    final case class Failure(reason: String)         extends DownloadResponse

  sealed trait DeleteResponse extends Response
  object DeleteResponse:
    final case class Success(fileId: String)  extends DeleteResponse
    final case class NotFound(fileId: String) extends DeleteResponse
    final case class Failure(reason: String)  extends DeleteResponse

  sealed trait FileInfoResponse extends Response
  object FileInfoResponse:
    final case class Success(metadata: FileMetadata) extends FileInfoResponse
    final case class NotFound(fileId: String)        extends FileInfoResponse
    final case class Failure(reason: String)         extends FileInfoResponse
