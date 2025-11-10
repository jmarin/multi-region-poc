package com.jmarin.filemanager.domain

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class FileManagerStateSpec extends AnyWordSpec with Matchers:

  "FileManagerState" should {

    "be empty by default" in {
      val state = FileManagerState.empty

      state.isEmpty shouldBe true
      state.exists shouldBe false
      state.metadata shouldBe None
    }

    "track file metadata when set" in {
      val metadata = FileMetadata(
        fileId = "file-1",
        fileName = "test.txt",
        fileSize = 100L,
        contentType = "text/plain",
        uploadedAt = Instant.now(),
        checksum = "abc123",
        replicas = Set("us-east-1")
      )

      val state = FileManagerState.empty.withMetadata(metadata)

      state.isEmpty shouldBe false
      state.exists shouldBe true
      state.metadata shouldBe Some(metadata)
    }

    "add replicas to existing metadata" in {
      val metadata = FileMetadata(
        fileId = "file-1",
        fileName = "test.txt",
        fileSize = 100L,
        contentType = "text/plain",
        uploadedAt = Instant.now(),
        checksum = "abc123",
        replicas = Set("us-east-1")
      )

      val state        = FileManagerState.empty.withMetadata(metadata)
      val updatedState = state.withReplicaAdded("eu-west-1")

      updatedState.metadata.get.replicas should have size 2
      updatedState.metadata.get.replicas should contain allOf ("us-east-1", "eu-west-1")
    }

    "be cleared when cleared" in {
      val metadata = FileMetadata(
        fileId = "file-1",
        fileName = "test.txt",
        fileSize = 100L,
        contentType = "text/plain",
        uploadedAt = Instant.now(),
        checksum = "abc123",
        replicas = Set("us-east-1")
      )

      val state        = FileManagerState.empty.withMetadata(metadata)
      val clearedState = state.cleared

      clearedState.isEmpty shouldBe true
      clearedState.metadata shouldBe None
    }

    "handle withReplicaAdded when metadata is None" in {
      val state        = FileManagerState.empty
      val updatedState = state.withReplicaAdded("us-east-1")

      updatedState.isEmpty shouldBe true
      updatedState.metadata shouldBe None
    }
  }
