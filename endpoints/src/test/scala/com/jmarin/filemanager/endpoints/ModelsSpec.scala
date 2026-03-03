package com.jmarin.filemanager.endpoints

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import io.circe.syntax.*
import io.circe.parser.*

class ModelsSpec extends AnyWordSpec with Matchers:

  import Models.given

  "UploadFileResponse" should {
    "serialize to JSON and deserialize back" in {
      val response = UploadFileResponse(
        fileId = "file-1",
        fileName = "test.txt",
        contentType = "text/plain",
        contentLength = 1024L,
        s3Key = "us-east-1/file-1",
        status = "uploaded",
        message = "File uploaded successfully"
      )

      val json    = response.asJson
      val decoded = json.as[UploadFileResponse]

      decoded shouldBe Right(response)
    }
  }

  "FileInfoResponse" should {
    "serialize to JSON and deserialize back" in {
      val response = FileInfoResponse(
        fileId = "file-1",
        fileName = "test.txt",
        contentType = "text/plain",
        contentLength = 1024L,
        owner = "test-user",
        createdAt = "2024-01-01T00:00:00Z",
        replicas = List(
          ReplicaInfo(regionId = "us-east-1", s3Key = "us-east-1/file-1", status = "active")
        )
      )

      val json    = response.asJson
      val decoded = json.as[FileInfoResponse]

      decoded shouldBe Right(response)
    }

    "handle empty replicas list" in {
      val response = FileInfoResponse(
        fileId = "file-1",
        fileName = "test.txt",
        contentType = "text/plain",
        contentLength = 0L,
        owner = "user",
        createdAt = "2024-01-01T00:00:00Z",
        replicas = List.empty
      )

      val json    = response.asJson
      val decoded = json.as[FileInfoResponse]

      decoded shouldBe Right(response)
    }
  }

  "ReplicaInfo" should {
    "serialize to JSON and deserialize back" in {
      val replica = ReplicaInfo(
        regionId = "us-east-1",
        s3Key = "us-east-1/file-1",
        status = "active"
      )

      val json    = replica.asJson
      val decoded = json.as[ReplicaInfo]

      decoded shouldBe Right(replica)
    }
  }

  "DeleteFileResponse" should {
    "serialize to JSON and deserialize back" in {
      val response = DeleteFileResponse(
        fileId = "file-1",
        status = "deleted",
        message = "File deleted successfully"
      )

      val json    = response.asJson
      val decoded = json.as[DeleteFileResponse]

      decoded shouldBe Right(response)
    }
  }

  "ListFilesResponse" should {
    "serialize to JSON and deserialize back" in {
      val response = ListFilesResponse(
        files = List(
          FileInfoResponse(
            fileId = "file-1",
            fileName = "test.txt",
            contentType = "text/plain",
            contentLength = 100L,
            owner = "user",
            createdAt = "2024-01-01T00:00:00Z",
            replicas = List.empty
          )
        ),
        totalCount = 1,
        nextCursor = Some("cursor-123")
      )

      val json    = response.asJson
      val decoded = json.as[ListFilesResponse]

      decoded shouldBe Right(response)
    }

    "handle None nextCursor" in {
      val response = ListFilesResponse(
        files = List.empty,
        totalCount = 0,
        nextCursor = None
      )

      val json    = response.asJson
      val decoded = json.as[ListFilesResponse]

      decoded shouldBe Right(response)
    }
  }

  "DownloadFileResponse" should {
    "serialize to JSON and deserialize back" in {
      val response = DownloadFileResponse(
        fileId = "file-1",
        downloadUrl = "https://example.com/download",
        expiresAt = "2024-01-01T01:00:00Z"
      )

      val json    = response.asJson
      val decoded = json.as[DownloadFileResponse]

      decoded shouldBe Right(response)
    }
  }

  "HealthResponse" should {
    "serialize to JSON and deserialize back" in {
      val response = HealthResponse(
        status = "OK",
        version = "1.0.0",
        uptime = 12345L
      )

      val json    = response.asJson
      val decoded = json.as[HealthResponse]

      decoded shouldBe Right(response)
    }
  }

  "ErrorResponse" should {
    "serialize to JSON and deserialize back with details" in {
      val response = ErrorResponse(
        error = "NotFound",
        message = "File not found",
        details = Some("fileId: file-1")
      )

      val json    = response.asJson
      val decoded = json.as[ErrorResponse]

      decoded shouldBe Right(response)
    }

    "serialize to JSON and deserialize back without details" in {
      val response = ErrorResponse(
        error = "InternalError",
        message = "Something went wrong",
        details = None
      )

      val json    = response.asJson
      val decoded = json.as[ErrorResponse]

      decoded shouldBe Right(response)
    }
  }

  "ListFilesRequest" should {
    "serialize to JSON and deserialize back" in {
      val request = ListFilesRequest(
        owner = "test-user",
        limit = Some(10),
        cursor = Some("cursor-1")
      )

      val json    = request.asJson
      val decoded = json.as[ListFilesRequest]

      decoded shouldBe Right(request)
    }

    "have default values for limit and cursor" in {
      val request = ListFilesRequest(owner = "user")

      request.limit shouldBe None
      request.cursor shouldBe None
    }
  }
