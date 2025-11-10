package com.jmarin.filemanager.endpoints

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.*
import sttp.tapir.{Codec, CodecFormat, Schema}
import sttp.tapir.generic.auto.*

import java.io.File

// Request models
case class UploadFileRequest(
    file: File,
    owner: String
)

case class ListFilesRequest(
    owner: String,
    limit: Option[Int] = None,
    cursor: Option[String] = None
)

// Response models
case class UploadFileResponse(
    fileId: String,
    fileName: String,
    contentType: String,
    contentLength: Long,
    s3Key: String,
    status: String,
    message: String
)

case class FileInfoResponse(
    fileId: String,
    fileName: String,
    contentType: String,
    contentLength: Long,
    owner: String,
    createdAt: String,
    replicas: List[ReplicaInfo]
)

case class ReplicaInfo(
    regionId: String,
    s3Key: String,
    status: String
)

case class DeleteFileResponse(
    fileId: String,
    status: String,
    message: String
)

case class ListFilesResponse(
    files: List[FileInfoResponse],
    totalCount: Int,
    nextCursor: Option[String]
)

case class DownloadFileResponse(
    fileId: String,
    downloadUrl: String,
    expiresAt: String
)

case class HealthResponse(
    status: String,
    version: String,
    uptime: Long
)

case class ErrorResponse(
    error: String,
    message: String,
    details: Option[String] = None
)

// Circe codecs (not needed for UploadFileRequest - it's multipart)
object Models:
  given Encoder[ListFilesRequest] = deriveEncoder
  given Decoder[ListFilesRequest] = deriveDecoder

  given Encoder[UploadFileResponse] = deriveEncoder
  given Decoder[UploadFileResponse] = deriveDecoder

  given Encoder[ReplicaInfo] = deriveEncoder
  given Decoder[ReplicaInfo] = deriveDecoder

  given Encoder[FileInfoResponse] = deriveEncoder
  given Decoder[FileInfoResponse] = deriveDecoder

  given Encoder[DeleteFileResponse] = deriveEncoder
  given Decoder[DeleteFileResponse] = deriveDecoder

  given Encoder[ListFilesResponse] = deriveEncoder
  given Decoder[ListFilesResponse] = deriveDecoder

  given Encoder[DownloadFileResponse] = deriveEncoder
  given Decoder[DownloadFileResponse] = deriveDecoder

  given Encoder[HealthResponse] = deriveEncoder
  given Decoder[HealthResponse] = deriveDecoder

  given Encoder[ErrorResponse] = deriveEncoder
  given Decoder[ErrorResponse] = deriveDecoder
