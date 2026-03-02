package com.jmarin.filemanager.grpc

import com.jmarin.filemanager.persistence.FileManager
import com.jmarin.filemanager.storage.StorageService
import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity}
import org.apache.pekko.cluster.typed.{Cluster, Join}
import org.apache.pekko.persistence.typed.ReplicaId
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
    pekko.actor.allow-java-serialization = on
    pekko.actor.warn-about-java-serializer-usage = off
    pekko.actor.serialization-bindings {
      "com.jmarin.filemanager.domain.CborSerializable" = jackson-cbor
    }
    pekko.persistence.journal.plugin = "pekko.persistence.journal.inmem"
    pekko.persistence.snapshot-store.plugin = "pekko.persistence.snapshot-store.local"
    pekko.persistence.snapshot-store.local.dir = "target/test-snapshots-grpc"
    pekko.persistence.typed.replicated-event-sourcing {
      replicas = ["test-region"]
    }
    jdbc-read-journal {
      class = "org.apache.pekko.persistence.testkit.query.PersistenceTestKitReadJournalProvider"
    }
  """)

  private val testKit = ActorTestKit("FileManagerServiceImplSpec", config)

  given system: org.apache.pekko.actor.typed.ActorSystem[?] = testKit.system
  // Longer timeout needed for cluster sharding initialization and entity resolution
  override given patienceConfig: PatienceConfig             = PatienceConfig(timeout = 10.seconds, interval = 200.millis)

  import scala.concurrent.ExecutionContext.Implicits.global

  // Initialize cluster (single node) and sharding for testing
  private val cluster = Cluster(testKit.system)
  cluster.manager ! Join(cluster.selfMember.address)

  private val replicaId     = ReplicaId("test-region")
  private val allReplicaIds = Set(replicaId)

  // Initialize sharding for FileManager entities
  private val sharding = ClusterSharding(testKit.system)
  sharding.init(Entity(FileManager.TypeKey): entityContext =>
    FileManager(entityContext.entityId, replicaId, allReplicaIds))

  override def afterAll(): Unit =
    testKit.shutdownTestKit()

  // In-memory storage service for testing
  class InMemoryStorageService extends StorageService:
    import java.util.concurrent.atomic.AtomicReference

    private val uploadedFiles = new AtomicReference[Map[String, (String, Long)]](Map.empty)
    private val deletedFiles  = new AtomicReference[Set[String]](Set.empty)
    private val presignedUrls = new AtomicReference[Map[String, URL]](Map.empty)

    override def uploadFile(
        fileId: String,
        fileName: String,
        content: Source[ByteString, Any],
        contentLength: Long,
        contentType: String
    ): Future[String] =
      val s3Key = s"test-region/$fileId"
      uploadedFiles.updateAndGet(_ + (s3Key -> (fileName, contentLength)))
      Future.successful(s3Key)

    override def downloadFile(s3Key: String): Future[Source[ByteString, Any]] =
      Future.successful(Source.single(ByteString("test content")))

    override def deleteFile(s3Key: String): Future[Unit] =
      deletedFiles.updateAndGet(_ + s3Key)
      Future.successful(())

    override def fileExists(s3Key: String): Future[Boolean] =
      Future.successful(uploadedFiles.get().contains(s3Key))

    override def getFileSize(s3Key: String): Future[Long] =
      Future.successful(uploadedFiles.get().get(s3Key).map(_._2).getOrElse(0L))

    override def generatePresignedUrl(s3Key: String, duration: FiniteDuration): Future[URL] =
      val url = java.net.URI.create(s"https://test-bucket.s3.amazonaws.com/$s3Key?presigned=true").toURL
      presignedUrls.updateAndGet(_ + (s3Key -> url))
      Future.successful(url)

    // Accessor methods for tests
    def getUploadedFiles: Map[String, (String, Long)] = uploadedFiles.get()
    def getDeletedFiles: Set[String]                  = deletedFiles.get()
    def getPresignedUrls: Map[String, URL]            = presignedUrls.get()

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
      storageService.getUploadedFiles should contain key s3Key

      // Test presigned URL generation
      val urlFuture = storageService.generatePresignedUrl(s3Key, 1.hour)
      val url       = urlFuture.futureValue
      url.toString should include(s3Key)
      url.toString should include("presigned=true")

      // Test delete
      val deleteFuture = storageService.deleteFile(s3Key)
      deleteFuture.futureValue
      storageService.getDeletedFiles should contain(s3Key)
    }

    "return empty list for listFiles with page and pageSize parameters" in {
      val storageService = new InMemoryStorageService
      val service        = new FileManagerServiceImpl(testKit.system, storageService, "test-region")

      val request  = ListFilesRequest(owner = "owner", page = 1, pageSize = 50)
      val response = service.listFiles(request).futureValue

      response.files shouldBe empty
      response.totalCount shouldBe 0
    }

    "return empty list for listFiles with empty owner" in {
      val storageService = new InMemoryStorageService
      val service        = new FileManagerServiceImpl(testKit.system, storageService, "test-region")

      val request  = ListFilesRequest(owner = "")
      val response = service.listFiles(request).futureValue

      response.files shouldBe empty
      response.totalCount shouldBe 0
    }

    "register a file and get its info via gRPC" in {
      val storageService = new InMemoryStorageService
      val service        = new FileManagerServiceImpl(testKit.system, storageService, "test-region")

      // Register a file
      val registerRequest = RegisterFileRequest(
        fileId = "grpc-test-file-1",
        name = "test-document.pdf",
        s3Key = "test-region/grpc-test-file-1",
        sizeBytes = 2048L,
        mimeType = "application/pdf",
        owner = "test-user",
        allowedRegions = Seq.empty,
        region = "test-region"
      )

      val registerResponse = service.registerFile(registerRequest).futureValue

      registerResponse.status shouldBe ResponseStatus.SUCCESS
      registerResponse.fileId shouldBe "grpc-test-file-1"
      registerResponse.metadata shouldBe defined
      registerResponse.metadata.get.fileId shouldBe "grpc-test-file-1"
      registerResponse.metadata.get.name shouldBe "test-document.pdf"
      registerResponse.metadata.get.sizeBytes shouldBe 2048L
      registerResponse.metadata.get.replicas should not be empty

      // Now get file info
      val infoRequest  = GetFileInfoRequest(fileId = "grpc-test-file-1")
      val infoResponse = service.getFileInfo(infoRequest).futureValue

      infoResponse.found shouldBe true
      infoResponse.metadata shouldBe defined
      infoResponse.metadata.get.fileId shouldBe "grpc-test-file-1"
    }

    "return not found for non-existent file info" in {
      val storageService = new InMemoryStorageService
      val service        = new FileManagerServiceImpl(testKit.system, storageService, "test-region")

      val request  = GetFileInfoRequest(fileId = "non-existent-file")
      val response = service.getFileInfo(request).futureValue

      response.found shouldBe false
      response.metadata shouldBe None
    }

    "delete a registered file" in {
      val storageService = new InMemoryStorageService
      val service        = new FileManagerServiceImpl(testKit.system, storageService, "test-region")

      // First register a file
      val registerRequest = RegisterFileRequest(
        fileId = "grpc-delete-test",
        name = "to-delete.txt",
        s3Key = "test-region/grpc-delete-test",
        sizeBytes = 100L,
        mimeType = "text/plain",
        owner = "user",
        allowedRegions = Seq.empty,
        region = "test-region"
      )
      service.registerFile(registerRequest).futureValue

      // Delete the file
      val deleteRequest  = DeleteFileRequest(fileId = "grpc-delete-test")
      val deleteResponse = service.deleteFile(deleteRequest).futureValue

      deleteResponse.success shouldBe true
    }

    "return failure when deleting non-existent file" in {
      val storageService = new InMemoryStorageService
      val service        = new FileManagerServiceImpl(testKit.system, storageService, "test-region")

      val deleteRequest  = DeleteFileRequest(fileId = "non-existent-delete")
      val deleteResponse = service.deleteFile(deleteRequest).futureValue

      deleteResponse.success shouldBe false
      deleteResponse.errorMessage should not be empty
    }

    "download a registered file and get presigned URL" in {
      val storageService = new InMemoryStorageService
      val service        = new FileManagerServiceImpl(testKit.system, storageService, "test-region")

      // First register a file
      val registerRequest = RegisterFileRequest(
        fileId = "grpc-download-test",
        name = "download.pdf",
        s3Key = "test-region/grpc-download-test",
        sizeBytes = 4096L,
        mimeType = "application/pdf",
        owner = "user",
        allowedRegions = Seq.empty,
        region = "test-region"
      )
      service.registerFile(registerRequest).futureValue

      // Download the file (get presigned URL)
      val downloadRequest  = DownloadFileRequest(fileId = "grpc-download-test", region = "test-region")
      val downloadResponse = service.downloadFile(downloadRequest).futureValue

      downloadResponse.status shouldBe ResponseStatus.SUCCESS
      downloadResponse.presignedUrl should not be empty
      downloadResponse.s3Key should include("grpc-download-test")
    }

    "return not found when downloading non-existent file" in {
      val storageService = new InMemoryStorageService
      val service        = new FileManagerServiceImpl(testKit.system, storageService, "test-region")

      val downloadRequest  = DownloadFileRequest(fileId = "non-existent-download", region = "test-region")
      val downloadResponse = service.downloadFile(downloadRequest).futureValue

      downloadResponse.status shouldBe ResponseStatus.NOT_FOUND
    }
  }

  "InMemoryStorageService" should {
    "track uploaded files" in {
      val storage = new InMemoryStorageService

      val s3Key1 = storage.uploadFile("file1", "test1.txt", Source.empty, 100L, "text/plain").futureValue
      val s3Key2 = storage.uploadFile("file2", "test2.txt", Source.empty, 200L, "text/plain").futureValue

      storage.getUploadedFiles should have size 2
      storage.getUploadedFiles(s3Key1) shouldBe ("test1.txt", 100L)
      storage.getUploadedFiles(s3Key2) shouldBe ("test2.txt", 200L)
    }

    "track deleted files" in {
      val storage = new InMemoryStorageService

      storage.deleteFile("key1").futureValue
      storage.deleteFile("key2").futureValue

      storage.getDeletedFiles should have size 2
      storage.getDeletedFiles should contain("key1")
      storage.getDeletedFiles should contain("key2")
    }

    "generate presigned URLs" in {
      val storage = new InMemoryStorageService

      val url = storage.generatePresignedUrl("test-key", 1.hour).futureValue

      url.toString should include("test-key")
      url.toString should include("presigned=true")
      storage.getPresignedUrls should contain key "test-key"
    }

    "report file exists after upload" in {
      val storage = new InMemoryStorageService

      val s3Key = storage.uploadFile("file1", "test.txt", Source.empty, 100L, "text/plain").futureValue

      storage.fileExists(s3Key).futureValue shouldBe true
      storage.fileExists("non-existent-key").futureValue shouldBe false
    }

    "return file size for uploaded files" in {
      val storage = new InMemoryStorageService

      val s3Key = storage.uploadFile("file1", "test.txt", Source.empty, 512L, "text/plain").futureValue

      storage.getFileSize(s3Key).futureValue shouldBe 512L
      storage.getFileSize("non-existent-key").futureValue shouldBe 0L
    }

    "download files returning content" in {
      val storage = new InMemoryStorageService

      val source = storage.downloadFile("any-key").futureValue
      source should not be null
    }
  }
