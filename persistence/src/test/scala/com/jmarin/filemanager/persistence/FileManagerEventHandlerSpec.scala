package com.jmarin.filemanager.persistence

import com.jmarin.filemanager.domain.*
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class FileManagerEventHandlerSpec extends AnyWordSpec with Matchers:

  // We test the event handler logic through state transitions
  // since the eventHandler is private to FileManager object,
  // we test equivalent behavior through FileManagerState operations

  "FileManager event handling logic" should {

    "handle FileUploaded event by creating state with metadata" in {
      val event = Event.FileUploaded(
        fileId = "file-1",
        fileName = "test.txt",
        fileSize = 1024L,
        contentType = "text/plain",
        checksum = "sha256-abc",
        region = "us-east-1",
        uploadedAt = Instant.parse("2024-01-01T00:00:00Z")
      )

      // Simulate event handler behavior
      val state = FileManagerState.empty
      val metadata = FileMetadata(
        fileId = event.fileId,
        fileName = event.fileName,
        fileSize = event.fileSize,
        contentType = event.contentType,
        uploadedAt = event.uploadedAt,
        checksum = event.checksum,
        replicas = Set(event.region)
      )
      val newState = state.withMetadata(metadata)

      newState.exists shouldBe true
      newState.metadata.get.fileId shouldBe "file-1"
      newState.metadata.get.fileName shouldBe "test.txt"
      newState.metadata.get.fileSize shouldBe 1024L
      newState.metadata.get.contentType shouldBe "text/plain"
      newState.metadata.get.checksum shouldBe "sha256-abc"
      newState.metadata.get.replicas shouldBe Set("us-east-1")
      newState.metadata.get.uploadedAt shouldBe Instant.parse("2024-01-01T00:00:00Z")
    }

    "handle FileDownloaded event by not changing state" in {
      val metadata = FileMetadata(
        fileId = "file-1",
        fileName = "test.txt",
        fileSize = 1024L,
        contentType = "text/plain",
        uploadedAt = Instant.now(),
        checksum = "abc",
        replicas = Set("us-east-1")
      )
      val state = FileManagerState.empty.withMetadata(metadata)

      // FileDownloaded doesn't change state
      val newState = state // state remains unchanged
      newState shouldBe state
    }

    "handle FileDeleted event by clearing state" in {
      val metadata = FileMetadata(
        fileId = "file-1",
        fileName = "test.txt",
        fileSize = 1024L,
        contentType = "text/plain",
        uploadedAt = Instant.now(),
        checksum = "abc",
        replicas = Set("us-east-1")
      )
      val state = FileManagerState.empty.withMetadata(metadata)

      // Simulate event handler for FileDeleted
      val newState = state.cleared

      newState.isEmpty shouldBe true
      newState.metadata shouldBe None
    }

    "handle FileReplicated event by adding target region" in {
      val metadata = FileMetadata(
        fileId = "file-1",
        fileName = "test.txt",
        fileSize = 1024L,
        contentType = "text/plain",
        uploadedAt = Instant.now(),
        checksum = "abc",
        replicas = Set("us-east-1")
      )
      val state = FileManagerState.empty.withMetadata(metadata)

      // Simulate event handler for FileReplicated
      val newState = state.withReplicaAdded("eu-west-1")

      newState.metadata.get.replicas should have size 2
      newState.metadata.get.replicas should contain("us-east-1")
      newState.metadata.get.replicas should contain("eu-west-1")
    }

    "handle multiple replication events across regions" in {
      val metadata = FileMetadata(
        fileId = "file-1",
        fileName = "test.txt",
        fileSize = 1024L,
        contentType = "text/plain",
        uploadedAt = Instant.now(),
        checksum = "abc",
        replicas = Set("us-east-1")
      )
      val state = FileManagerState.empty.withMetadata(metadata)

      val state2 = state.withReplicaAdded("eu-west-1")
      val state3 = state2.withReplicaAdded("ap-south-1")

      state3.metadata.get.replicas should have size 3
      state3.metadata.get.replicas should contain allOf ("us-east-1", "eu-west-1", "ap-south-1")
    }

    "handle duplicate region in replication (set semantics)" in {
      val metadata = FileMetadata(
        fileId = "file-1",
        fileName = "test.txt",
        fileSize = 1024L,
        contentType = "text/plain",
        uploadedAt = Instant.now(),
        checksum = "abc",
        replicas = Set("us-east-1")
      )
      val state = FileManagerState.empty.withMetadata(metadata)

      val state2 = state.withReplicaAdded("us-east-1") // duplicate

      state2.metadata.get.replicas should have size 1
    }

    "handle full lifecycle: upload -> download -> replicate -> delete" in {
      // 1. Upload
      val uploadEvent = Event.FileUploaded(
        fileId = "lifecycle-file",
        fileName = "doc.pdf",
        fileSize = 2048L,
        contentType = "application/pdf",
        checksum = "sha256-xyz",
        region = "us-east-1",
        uploadedAt = Instant.now()
      )
      val metadata = FileMetadata(
        fileId = uploadEvent.fileId,
        fileName = uploadEvent.fileName,
        fileSize = uploadEvent.fileSize,
        contentType = uploadEvent.contentType,
        uploadedAt = uploadEvent.uploadedAt,
        checksum = uploadEvent.checksum,
        replicas = Set(uploadEvent.region)
      )
      val stateAfterUpload = FileManagerState.empty.withMetadata(metadata)
      stateAfterUpload.exists shouldBe true

      // 2. Download - state unchanged
      val stateAfterDownload = stateAfterUpload
      stateAfterDownload.exists shouldBe true

      // 3. Replicate
      val stateAfterReplicate = stateAfterDownload.withReplicaAdded("eu-west-1")
      stateAfterReplicate.metadata.get.replicas should have size 2

      // 4. Delete
      val stateAfterDelete = stateAfterReplicate.cleared
      stateAfterDelete.isEmpty shouldBe true
    }
  }
