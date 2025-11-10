package com.jmarin.filemanager.endpoints

import sttp.tapir.*
import sttp.tapir.json.circe.*
import sttp.tapir.generic.auto.*
import io.circe.generic.auto.*

object FileManagerEndpoints:

  // Base path for all file manager endpoints
  private val baseEndpoint = endpoint
    .in("api" / "v1" / "files")
    .errorOut(jsonBody[ErrorResponse])

  // Upload file endpoint - handles multipart upload, stores to S3/MinIO, registers with backend
  val uploadFile: PublicEndpoint[UploadFileRequest, ErrorResponse, UploadFileResponse, Any] =
    baseEndpoint.post
      .in("upload")
      .in(
        multipartBody[UploadFileRequest]
          .description("File upload with metadata")
      )
      .out(jsonBody[UploadFileResponse])
      .name("uploadFile")
      .description("Upload a file to storage (S3/MinIO) and register it with the backend")
      .tag("Files")

  // Get file info endpoint
  val getFileInfo: PublicEndpoint[String, ErrorResponse, FileInfoResponse, Any] =
    baseEndpoint.get
      .in(path[String]("fileId"))
      .out(jsonBody[FileInfoResponse])
      .name("getFileInfo")
      .description("Get metadata for a specific file")
      .tag("Files")

  // Delete file endpoint
  val deleteFile: PublicEndpoint[String, ErrorResponse, DeleteFileResponse, Any] =
    baseEndpoint.delete
      .in(path[String]("fileId"))
      .out(jsonBody[DeleteFileResponse])
      .name("deleteFile")
      .description("Delete a file and its metadata")
      .tag("Files")

  // List files endpoint
  val listFiles: PublicEndpoint[ListFilesRequest, ErrorResponse, ListFilesResponse, Any] =
    baseEndpoint.get
      .in("list")
      .in(query[String]("owner"))
      .in(query[Option[Int]]("limit"))
      .in(query[Option[String]]("cursor"))
      .mapIn { case (owner, limit, cursor) =>
        ListFilesRequest(owner, limit, cursor)
      }(req => (req.owner, req.limit, req.cursor))
      .out(jsonBody[ListFilesResponse])
      .name("listFiles")
      .description("List files for a specific owner")
      .tag("Files")

  // Download file endpoint (get presigned URL)
  val downloadFile: PublicEndpoint[String, ErrorResponse, DownloadFileResponse, Any] =
    baseEndpoint.get
      .in(path[String]("fileId") / "download")
      .out(jsonBody[DownloadFileResponse])
      .name("downloadFile")
      .description("Get a presigned URL to download a file")
      .tag("Files")

  // Health check endpoint
  val health: PublicEndpoint[Unit, ErrorResponse, HealthResponse, Any] =
    endpoint.get
      .in("health")
      .errorOut(jsonBody[ErrorResponse])
      .out(jsonBody[HealthResponse])
      .name("health")
      .description("Health check endpoint")
      .tag("System")

  // All endpoints
  val all: List[AnyEndpoint] = List(
    uploadFile,
    getFileInfo,
    deleteFile,
    listFiles,
    downloadFile,
    health
  )
