package com.jmarin.filemanager.persistence

import com.jmarin.filemanager.domain.*
import org.apache.pekko.actor.testkit.typed.scaladsl.{ActorTestKit, ScalaTestWithActorTestKit}
import org.apache.pekko.persistence.testkit.scaladsl.EventSourcedBehaviorTestKit
import org.apache.pekko.persistence.typed.{PersistenceId, ReplicaId}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.matchers.should.Matchers
import com.typesafe.config.ConfigFactory

import java.time.Instant

class FileManagerSpec
    extends ScalaTestWithActorTestKit(
      EventSourcedBehaviorTestKit.config.withFallback(
        ConfigFactory.parseString("""
          pekko.actor.provider = local
          pekko.actor.allow-java-serialization = on
          pekko.actor.warn-about-java-serializer-usage = off
          pekko.actor.serialization-bindings {
            "com.jmarin.filemanager.domain.CborSerializable" = jackson-cbor
          }
          jdbc-read-journal {
            class = "org.apache.pekko.persistence.testkit.query.PersistenceTestKitReadJournalProvider"
          }
        """)
      )
    )
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterEach:

  private val replicaId     = ReplicaId("us-east-1")
  private val allReplicaIds = Set(ReplicaId("us-east-1"), ReplicaId("eu-west-1"), ReplicaId("ap-south-1"))

  private val eventSourcedTestKit = EventSourcedBehaviorTestKit[Command, Event, FileManagerState](
    system,
    FileManager("test-file-1", replicaId, allReplicaIds)
  )

  override protected def beforeEach(): Unit =
    super.beforeEach()
    eventSourcedTestKit.clear()

  "FileManager" should {

    "upload a new file" in {
      val result = eventSourcedTestKit.runCommand[Command.UploadResponse]: replyTo =>
        Command.UploadFile(
          fileId = "test-file-1",
          fileName = "document.pdf",
          fileSize = 2048L,
          contentType = "application/pdf",
          checksum = "sha256-abc123",
          region = "us-east-1",
          replyTo = replyTo
        )

      result.reply shouldBe a[Command.UploadResponse.Success]
      val success = result.reply.asInstanceOf[Command.UploadResponse.Success]
      success.metadata.fileId shouldBe "test-file-1"
      success.metadata.fileName shouldBe "document.pdf"
      success.metadata.fileSize shouldBe 2048L
      success.metadata.contentType shouldBe "application/pdf"
      success.metadata.replicas should contain("us-east-1")

      result.event shouldBe a[Event.FileUploaded]
      result.stateOfType[FileManagerState].exists shouldBe true
    }

    "handle upload to existing file as replication" in {
      // First upload
      eventSourcedTestKit.runCommand[Command.UploadResponse]: replyTo =>
        Command.UploadFile("test-file-1", "doc.pdf", 1024L, "application/pdf", "abc", "us-east-1", replyTo)

      // Second upload (replication)
      val result = eventSourcedTestKit.runCommand[Command.UploadResponse]: replyTo =>
        Command.UploadFile("test-file-1", "doc.pdf", 1024L, "application/pdf", "abc", "eu-west-1", replyTo)

      result.reply shouldBe a[Command.UploadResponse.Success]
      result.event shouldBe a[Event.FileReplicated]
      val replicated = result.event.asInstanceOf[Event.FileReplicated]
      replicated.targetRegion shouldBe "eu-west-1"
    }

    "download an existing file" in {
      // Upload first
      eventSourcedTestKit.runCommand[Command.UploadResponse]: replyTo =>
        Command.UploadFile("test-file-1", "doc.pdf", 1024L, "application/pdf", "abc", "us-east-1", replyTo)

      // Download
      val result = eventSourcedTestKit.runCommand[Command.DownloadResponse]: replyTo =>
        Command.DownloadFile("test-file-1", replyTo)

      result.reply shouldBe a[Command.DownloadResponse.Success]
      val success = result.reply.asInstanceOf[Command.DownloadResponse.Success]
      success.metadata.fileId shouldBe "test-file-1"
      result.event shouldBe a[Event.FileDownloaded]
    }

    "return NotFound when downloading non-existent file" in {
      val result = eventSourcedTestKit.runCommand[Command.DownloadResponse]: replyTo =>
        Command.DownloadFile("test-file-1", replyTo)

      result.reply shouldBe a[Command.DownloadResponse.NotFound]
      result.hasNoEvents shouldBe true
    }

    "delete an existing file" in {
      // Upload first
      eventSourcedTestKit.runCommand[Command.UploadResponse]: replyTo =>
        Command.UploadFile("test-file-1", "doc.pdf", 1024L, "application/pdf", "abc", "us-east-1", replyTo)

      // Delete
      val result = eventSourcedTestKit.runCommand[Command.DeleteResponse]: replyTo =>
        Command.DeleteFile("test-file-1", replyTo)

      result.reply shouldBe Command.DeleteResponse.Success("test-file-1")
      result.event shouldBe a[Event.FileDeleted]
      result.stateOfType[FileManagerState].isEmpty shouldBe true
    }

    "return NotFound when deleting non-existent file" in {
      val result = eventSourcedTestKit.runCommand[Command.DeleteResponse]: replyTo =>
        Command.DeleteFile("test-file-1", replyTo)

      result.reply shouldBe a[Command.DeleteResponse.NotFound]
      result.hasNoEvents shouldBe true
    }

    "get file info for existing file" in {
      // Upload first
      eventSourcedTestKit.runCommand[Command.UploadResponse]: replyTo =>
        Command.UploadFile("test-file-1", "doc.pdf", 1024L, "application/pdf", "abc", "us-east-1", replyTo)

      // Get info
      val result = eventSourcedTestKit.runCommand[Command.FileInfoResponse]: replyTo =>
        Command.GetFileInfo("test-file-1", replyTo)

      result.reply shouldBe a[Command.FileInfoResponse.Success]
      val success = result.reply.asInstanceOf[Command.FileInfoResponse.Success]
      success.metadata.fileName shouldBe "doc.pdf"
      result.hasNoEvents shouldBe true
    }

    "return NotFound when getting info for non-existent file" in {
      val result = eventSourcedTestKit.runCommand[Command.FileInfoResponse]: replyTo =>
        Command.GetFileInfo("test-file-1", replyTo)

      result.reply shouldBe a[Command.FileInfoResponse.NotFound]
      result.hasNoEvents shouldBe true
    }

    "handle full lifecycle: upload, download, replicate, get info, delete" in {
      // 1. Upload
      val uploadResult = eventSourcedTestKit.runCommand[Command.UploadResponse]: replyTo =>
        Command.UploadFile("test-file-1", "report.pdf", 4096L, "application/pdf", "sha256-xyz", "us-east-1", replyTo)
      uploadResult.reply shouldBe a[Command.UploadResponse.Success]

      // 2. Download
      val downloadResult = eventSourcedTestKit.runCommand[Command.DownloadResponse]: replyTo =>
        Command.DownloadFile("test-file-1", replyTo)
      downloadResult.reply shouldBe a[Command.DownloadResponse.Success]

      // 3. Replicate (upload to another region)
      val replicateResult = eventSourcedTestKit.runCommand[Command.UploadResponse]: replyTo =>
        Command.UploadFile("test-file-1", "report.pdf", 4096L, "application/pdf", "sha256-xyz", "eu-west-1", replyTo)
      replicateResult.reply shouldBe a[Command.UploadResponse.Success]
      replicateResult.event shouldBe a[Event.FileReplicated]

      // 4. Get info - should show multiple replicas
      val infoResult = eventSourcedTestKit.runCommand[Command.FileInfoResponse]: replyTo =>
        Command.GetFileInfo("test-file-1", replyTo)
      infoResult.reply shouldBe a[Command.FileInfoResponse.Success]

      // 5. Delete
      val deleteResult = eventSourcedTestKit.runCommand[Command.DeleteResponse]: replyTo =>
        Command.DeleteFile("test-file-1", replyTo)
      deleteResult.reply shouldBe Command.DeleteResponse.Success("test-file-1")
      deleteResult.stateOfType[FileManagerState].isEmpty shouldBe true

      // 6. Verify file no longer exists
      val infoAfterDelete = eventSourcedTestKit.runCommand[Command.FileInfoResponse]: replyTo =>
        Command.GetFileInfo("test-file-1", replyTo)
      infoAfterDelete.reply shouldBe a[Command.FileInfoResponse.NotFound]
    }
  }
