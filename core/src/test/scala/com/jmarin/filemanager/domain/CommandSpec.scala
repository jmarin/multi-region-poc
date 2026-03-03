package com.jmarin.filemanager.domain

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class CommandSpec extends AnyWordSpec with Matchers:

  "Command response types" should {

    "create UploadResponse.Success with metadata" in {
      val metadata = FileMetadata(
        fileId = "file-1",
        fileName = "test.txt",
        fileSize = 100L,
        contentType = "text/plain",
        uploadedAt = Instant.now(),
        checksum = "abc",
        replicas = Set("us-east-1")
      )
      val response = Command.UploadResponse.Success(metadata)

      response.metadata shouldBe metadata
      response shouldBe a[Command.UploadResponse]
      response shouldBe a[Command.Response]
      response shouldBe a[CborSerializable]
    }

    "create UploadResponse.Failure with reason" in {
      val response = Command.UploadResponse.Failure("upload error")

      response.reason shouldBe "upload error"
      response shouldBe a[Command.UploadResponse]
    }

    "create DownloadResponse.Success with metadata" in {
      val metadata = FileMetadata(
        fileId = "file-1",
        fileName = "test.txt",
        fileSize = 100L,
        contentType = "text/plain",
        uploadedAt = Instant.now(),
        checksum = "abc",
        replicas = Set("us-east-1")
      )
      val response = Command.DownloadResponse.Success(metadata)

      response.metadata shouldBe metadata
      response shouldBe a[Command.DownloadResponse]
    }

    "create DownloadResponse.NotFound with fileId" in {
      val response = Command.DownloadResponse.NotFound("missing-file")

      response.fileId shouldBe "missing-file"
      response shouldBe a[Command.DownloadResponse]
    }

    "create DownloadResponse.Failure with reason" in {
      val response = Command.DownloadResponse.Failure("download error")

      response.reason shouldBe "download error"
      response shouldBe a[Command.DownloadResponse]
    }

    "create DeleteResponse.Success with fileId" in {
      val response = Command.DeleteResponse.Success("file-1")

      response.fileId shouldBe "file-1"
      response shouldBe a[Command.DeleteResponse]
    }

    "create DeleteResponse.NotFound with fileId" in {
      val response = Command.DeleteResponse.NotFound("missing-file")

      response.fileId shouldBe "missing-file"
      response shouldBe a[Command.DeleteResponse]
    }

    "create DeleteResponse.Failure with reason" in {
      val response = Command.DeleteResponse.Failure("delete error")

      response.reason shouldBe "delete error"
      response shouldBe a[Command.DeleteResponse]
    }

    "create FileInfoResponse.Success with metadata" in {
      val metadata = FileMetadata(
        fileId = "file-1",
        fileName = "test.txt",
        fileSize = 100L,
        contentType = "text/plain",
        uploadedAt = Instant.now(),
        checksum = "abc",
        replicas = Set("us-east-1")
      )
      val response = Command.FileInfoResponse.Success(metadata)

      response.metadata shouldBe metadata
      response shouldBe a[Command.FileInfoResponse]
    }

    "create FileInfoResponse.NotFound with fileId" in {
      val response = Command.FileInfoResponse.NotFound("missing-file")

      response.fileId shouldBe "missing-file"
      response shouldBe a[Command.FileInfoResponse]
    }

    "create FileInfoResponse.Failure with reason" in {
      val response = Command.FileInfoResponse.Failure("info error")

      response.reason shouldBe "info error"
      response shouldBe a[Command.FileInfoResponse]
    }
  }
