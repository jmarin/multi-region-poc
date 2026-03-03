package com.jmarin.filemanager.domain

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class EventSpec extends AnyWordSpec with Matchers:

  "Event" should {

    "create FileUploaded event with all fields" in {
      val now = Instant.now()
      val event = Event.FileUploaded(
        fileId = "file-1",
        fileName = "test.txt",
        fileSize = 1024L,
        contentType = "text/plain",
        checksum = "sha256-abc",
        region = "us-east-1",
        uploadedAt = now
      )

      event.fileId shouldBe "file-1"
      event.fileName shouldBe "test.txt"
      event.fileSize shouldBe 1024L
      event.contentType shouldBe "text/plain"
      event.checksum shouldBe "sha256-abc"
      event.region shouldBe "us-east-1"
      event.uploadedAt shouldBe now
      event shouldBe a[Event]
      event shouldBe a[CborSerializable]
    }

    "create FileDownloaded event with all fields" in {
      val now = Instant.now()
      val event = Event.FileDownloaded(
        fileId = "file-1",
        region = "eu-west-1",
        downloadedAt = now
      )

      event.fileId shouldBe "file-1"
      event.region shouldBe "eu-west-1"
      event.downloadedAt shouldBe now
      event shouldBe a[Event]
      event shouldBe a[CborSerializable]
    }

    "create FileDeleted event with all fields" in {
      val now = Instant.now()
      val event = Event.FileDeleted(
        fileId = "file-1",
        deletedAt = now
      )

      event.fileId shouldBe "file-1"
      event.deletedAt shouldBe now
      event shouldBe a[Event]
      event shouldBe a[CborSerializable]
    }

    "create FileReplicated event with all fields" in {
      val now = Instant.now()
      val event = Event.FileReplicated(
        fileId = "file-1",
        sourceRegion = "us-east-1",
        targetRegion = "eu-west-1",
        replicatedAt = now
      )

      event.fileId shouldBe "file-1"
      event.sourceRegion shouldBe "us-east-1"
      event.targetRegion shouldBe "eu-west-1"
      event.replicatedAt shouldBe now
      event shouldBe a[Event]
      event shouldBe a[CborSerializable]
    }
  }
