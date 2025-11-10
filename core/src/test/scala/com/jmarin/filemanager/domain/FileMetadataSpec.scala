package com.jmarin.filemanager.domain

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class FileMetadataSpec extends AnyWordSpec with Matchers:

  "FileMetadata" should {

    "be created with all required fields" in {
      val fileId      = "test-file-123"
      val fileName    = "test.txt"
      val fileSize    = 1024L
      val contentType = "text/plain"
      val checksum    = "abc123"
      val region      = "us-east-1"

      val metadata = FileMetadata.create(
        fileId = fileId,
        fileName = fileName,
        fileSize = fileSize,
        contentType = contentType,
        checksum = checksum,
        region = region
      )

      metadata.fileId shouldBe fileId
      metadata.fileName shouldBe fileName
      metadata.fileSize shouldBe fileSize
      metadata.contentType shouldBe contentType
      metadata.checksum shouldBe checksum
      metadata.replicas should contain(region)
      metadata.uploadedAt should not be null
    }

    "have a single replica when created" in {
      val metadata = FileMetadata.create(
        fileId = "file-1",
        fileName = "test.txt",
        fileSize = 100L,
        contentType = "text/plain",
        checksum = "abc",
        region = "us-east-1"
      )

      metadata.replicas should have size 1
      metadata.replicas should contain("us-east-1")
    }

    "support adding replicas" in {
      val metadata = FileMetadata.create(
        fileId = "file-1",
        fileName = "test.txt",
        fileSize = 100L,
        contentType = "text/plain",
        checksum = "abc",
        region = "us-east-1"
      )

      val updated = metadata.copy(replicas = metadata.replicas + "eu-west-1")

      updated.replicas should have size 2
      updated.replicas should contain allOf ("us-east-1", "eu-west-1")
    }
  }
