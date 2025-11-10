package com.jmarin.filemanager.grpc

import com.jmarin.filemanager.storage.StorageService
import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.net.URL
import scala.concurrent.Future
import scala.concurrent.duration.*

class FileManagerServiceImplSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll with ScalaFutures:

  private val config = com.typesafe.config.ConfigFactory.parseString("""
    pekko.actor.provider = cluster
    pekko.remote.artery.canonical.port = 0
    pekko.remote.artery.canonical.hostname = 127.0.0.1
    pekko.cluster.jmx.multi-mbeans-in-same-jvm = on
  """)

  private val testKit = ActorTestKit("FileManagerServiceImplSpec", config)

  given system: org.apache.pekko.actor.typed.ActorSystem[?] = testKit.system
  override given patienceConfig: PatienceConfig             = PatienceConfig(timeout = 3.seconds, interval = 100.millis)

  import scala.concurrent.ExecutionContext.Implicits.global

  override def afterAll(): Unit =
    testKit.shutdownTestKit()

  // In-memory storage service for testing
  class InMemoryStorageService extends StorageService:
    var uploadedFiles: Map[String, (String, Long)] = Map.empty
    var deletedFiles: Set[String]                  = Set.empty
    var presignedUrls: Map[String, URL]            = Map.empty

    override def uploadFile(
        fileId: String,
        fileName: String,
        content: Source[ByteString, Any],
        contentLength: Long,
        contentType: String
    ): Future[String] =
      val s3Key = s"test-region/$fileId"
      uploadedFiles = uploadedFiles + (s3Key -> (fileName, contentLength))
      Future.successful(s3Key)

    override def downloadFile(s3Key: String): Future[Source[ByteString, Any]] =
      Future.successful(Source.single(ByteString("test content")))

    override def deleteFile(s3Key: String): Future[Unit] =
      deletedFiles = deletedFiles + s3Key
      Future.successful(())

    override def fileExists(s3Key: String): Future[Boolean] =
      Future.successful(uploadedFiles.contains(s3Key))

    override def getFileSize(s3Key: String): Future[Long] =
      Future.successful(uploadedFiles.get(s3Key).map(_._2).getOrElse(0L))

    override def generatePresignedUrl(s3Key: String, duration: FiniteDuration): Future[URL] =
      val url = URL.of(java.net.URI.create(s"https://test-bucket.s3.amazonaws.com/$s3Key?presigned=true"), null)
      presignedUrls = presignedUrls + (s3Key -> url)
      Future.successful(url)

  "FileManagerServiceImpl" should {

    "be instantiated with in-memory storage service" in {
      val storageService = new InMemoryStorageService
      val service        = new FileManagerServiceImpl(testKit.system, storageService, "test-region")

      service should not be null
      storageService should not be null
    }

    "return empty list for listFiles (placeholder implementation)" in {
      val storageService = new InMemoryStorageService
      val service        = new FileManagerServiceImpl(testKit.system, storageService, "test-region")

      val request  = ListFilesRequest(owner = "test-user")
      val response = service.listFiles(request).futureValue

      response.files shouldBe empty
      response.totalCount shouldBe 0
    }

    "handle storage service operations" in {
      val storageService = new InMemoryStorageService

      // Test upload
      val uploadFuture = storageService.uploadFile(
        fileId = "test-123",
        fileName = "test.txt",
        content = Source.single(ByteString("test")),
        contentLength = 4L,
        contentType = "text/plain"
      )
      val s3Key        = uploadFuture.futureValue
      s3Key shouldBe "test-region/test-123"
      storageService.uploadedFiles should contain key s3Key

      // Test presigned URL generation
      val urlFuture = storageService.generatePresignedUrl(s3Key, 1.hour)
      val url       = urlFuture.futureValue
      url.toString should include(s3Key)
      url.toString should include("presigned=true")

      // Test delete
      val deleteFuture = storageService.deleteFile(s3Key)
      deleteFuture.futureValue
      storageService.deletedFiles should contain(s3Key)
    }
  }

  "InMemoryStorageService" should {
    "track uploaded files" in {
      val storage = new InMemoryStorageService

      val s3Key1 = storage.uploadFile("file1", "test1.txt", Source.empty, 100L, "text/plain").futureValue
      val s3Key2 = storage.uploadFile("file2", "test2.txt", Source.empty, 200L, "text/plain").futureValue

      storage.uploadedFiles should have size 2
      storage.uploadedFiles(s3Key1) shouldBe ("test1.txt", 100L)
      storage.uploadedFiles(s3Key2) shouldBe ("test2.txt", 200L)
    }

    "track deleted files" in {
      val storage = new InMemoryStorageService

      storage.deleteFile("key1").futureValue
      storage.deleteFile("key2").futureValue

      storage.deletedFiles should have size 2
      storage.deletedFiles should contain("key1")
      storage.deletedFiles should contain("key2")
    }

    "generate presigned URLs" in {
      val storage = new InMemoryStorageService

      val url = storage.generatePresignedUrl("test-key", 1.hour).futureValue

      url.toString should include("test-key")
      url.toString should include("presigned=true")
      storage.presignedUrls should contain key "test-key"
    }
  }
