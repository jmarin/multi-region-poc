package com.jmarin.filemanager.api

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.jmarin.filemanager.endpoints.*
import com.jmarin.filemanager.storage.StorageService
import com.jmarin.filemanager.grpc.{
  FileManagerService,
  RegisterFileRequest,
  RegisterFileResponse,
  GetFileInfoRequest,
  GetFileInfoResponse,
  DeleteFileRequest,
  DeleteFileResponse as GrpcDeleteFileResponse,
  ListFilesRequest as GrpcListFilesRequest,
  ListFilesResponse as GrpcListFilesResponse,
  DownloadFileRequest,
  DownloadFileResponse as GrpcDownloadFileResponse,
  ResponseStatus,
  FileMetadata as GrpcFileMetadata,
  ReplicaInfo as GrpcReplicaInfo
}
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.net.URL
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration

class FileManagerHandlersSpec extends AnyWordSpec with Matchers:

  given ec: ExecutionContext = scala.concurrent.ExecutionContext.global

  private val testValidationConfig = ValidationConfig(
    maxFileSize = 10 * 1024 * 1024L, // 10 MB
    allowedContentTypes = Set("text/plain", "application/pdf", "application/octet-stream"),
    maxFilenameLength = 255
  )

  private val testApiConfig = ApiConfig(
    http = HttpConfig(host = "localhost", port = 8080),
    regionId = "us-east-1",
    validation = testValidationConfig
  )

  // Stub StorageService
  class StubStorageService extends StorageService:
    var uploadCalled = false
    var deleteCalled = false
    var lastUploadFileId: String = ""

    override def uploadFile(
        fileId: String,
        fileName: String,
        content: Source[ByteString, Any],
        contentLength: Long,
        contentType: String
    ): Future[String] =
      uploadCalled = true
      lastUploadFileId = fileId
      Future.successful(s"$fileId/$fileName")

    override def downloadFile(s3Key: String): Future[Source[ByteString, Any]] =
      Future.successful(Source.single(ByteString("content")))

    override def deleteFile(s3Key: String): Future[Unit] =
      deleteCalled = true
      Future.successful(())

    override def generatePresignedUrl(s3Key: String, expiration: FiniteDuration): Future[URL] =
      Future.successful(java.net.URI.create(s"https://storage.example.com/$s3Key").toURL)

    override def fileExists(s3Key: String): Future[Boolean] =
      Future.successful(true)

    override def getFileSize(s3Key: String): Future[Long] =
      Future.successful(1024L)

  // Stub gRPC service
  class StubGrpcService extends FileManagerService:
    var registerFileCalled = false
    var getFileInfoCalled = false
    var deleteFileCalled = false
    var listFilesCalled = false
    var downloadFileCalled = false

    // Configurable responses
    var registerResponse: RegisterFileResponse = RegisterFileResponse(
      fileId = "test-id",
      metadata = Some(GrpcFileMetadata(
        fileId = "test-id",
        name = "test.txt",
        sizeBytes = 100L,
        mimeType = "text/plain",
        s3Key = "us-east-1/test-id",
        owner = "user",
        createdAt = System.currentTimeMillis(),
        modifiedAt = System.currentTimeMillis(),
        status = "active",
        allowedRegions = Seq.empty,
        uploadedRegion = "us-east-1",
        replicas = Seq(GrpcReplicaInfo(regionId = "us-east-1", s3Key = "us-east-1/test-id", replicatedAt = System.currentTimeMillis()))
      )),
      status = ResponseStatus.SUCCESS,
      errorMessage = ""
    )

    var getFileInfoResponse: GetFileInfoResponse = GetFileInfoResponse(
      metadata = Some(GrpcFileMetadata(
        fileId = "test-id",
        name = "test.txt",
        sizeBytes = 100L,
        mimeType = "text/plain",
        s3Key = "us-east-1/test-id",
        owner = "user",
        createdAt = System.currentTimeMillis(),
        modifiedAt = System.currentTimeMillis(),
        status = "active",
        allowedRegions = Seq.empty,
        uploadedRegion = "us-east-1",
        replicas = Seq(GrpcReplicaInfo(regionId = "us-east-1", s3Key = "us-east-1/test-id", replicatedAt = System.currentTimeMillis()))
      )),
      found = true
    )

    var deleteResponse: GrpcDeleteFileResponse = GrpcDeleteFileResponse(success = true, errorMessage = "")

    var listResponse: GrpcListFilesResponse = GrpcListFilesResponse(files = Seq.empty, totalCount = 0)

    var downloadResponse: GrpcDownloadFileResponse = GrpcDownloadFileResponse(
      presignedUrl = "https://storage.example.com/file",
      s3Key = "us-east-1/test-id",
      status = ResponseStatus.SUCCESS,
      errorMessage = ""
    )

    override def registerFile(request: RegisterFileRequest): Future[RegisterFileResponse] =
      registerFileCalled = true
      Future.successful(registerResponse)

    override def getFileInfo(request: GetFileInfoRequest): Future[GetFileInfoResponse] =
      getFileInfoCalled = true
      Future.successful(getFileInfoResponse)

    override def deleteFile(request: DeleteFileRequest): Future[GrpcDeleteFileResponse] =
      deleteFileCalled = true
      Future.successful(deleteResponse)

    override def listFiles(request: GrpcListFilesRequest): Future[GrpcListFilesResponse] =
      listFilesCalled = true
      Future.successful(listResponse)

    override def downloadFile(request: DownloadFileRequest): Future[GrpcDownloadFileResponse] =
      downloadFileCalled = true
      Future.successful(downloadResponse)

  "FileManagerHandlers" should {

    "return health status" in {
      val storageService = new StubStorageService
      val grpcService = new StubGrpcService
      val handlers = FileManagerHandlers(testApiConfig, storageService, grpcService)

      val result = handlers.health.unsafeRunSync()

      result shouldBe a[Right[?, ?]]
      val response = result.toOption.get
      response.status shouldBe "OK"
      response.version shouldBe "1.0.0"
      response.uptime should be > 0L
    }

    "get file info successfully" in {
      val storageService = new StubStorageService
      val grpcService = new StubGrpcService
      val handlers = FileManagerHandlers(testApiConfig, storageService, grpcService)

      val result = handlers.getFileInfo("test-id").unsafeRunSync()

      result shouldBe a[Right[?, ?]]
      grpcService.getFileInfoCalled shouldBe true
      val response = result.toOption.get
      response.fileId shouldBe "test-id"
      response.fileName shouldBe "test.txt"
      response.replicas should not be empty
    }

    "return error when file info not found" in {
      val storageService = new StubStorageService
      val grpcService = new StubGrpcService
      grpcService.getFileInfoResponse = GetFileInfoResponse(metadata = None, found = false)
      val handlers = FileManagerHandlers(testApiConfig, storageService, grpcService)

      val result = handlers.getFileInfo("missing-id").unsafeRunSync()

      result shouldBe a[Left[?, ?]]
      val error = result.swap.toOption.get
      error.error shouldBe "NotFound"
    }

    "delete file successfully" in {
      val storageService = new StubStorageService
      val grpcService = new StubGrpcService
      val handlers = FileManagerHandlers(testApiConfig, storageService, grpcService)

      val result = handlers.deleteFile("test-id").unsafeRunSync()

      result shouldBe a[Right[?, ?]]
      grpcService.deleteFileCalled shouldBe true
      val response = result.toOption.get
      response.fileId shouldBe "test-id"
      response.status shouldBe "deleted"
    }

    "return error when delete fails" in {
      val storageService = new StubStorageService
      val grpcService = new StubGrpcService
      grpcService.deleteResponse = GrpcDeleteFileResponse(success = false, errorMessage = "File not found")
      val handlers = FileManagerHandlers(testApiConfig, storageService, grpcService)

      val result = handlers.deleteFile("missing-id").unsafeRunSync()

      result shouldBe a[Left[?, ?]]
      val error = result.swap.toOption.get
      error.error shouldBe "DeleteFailed"
    }

    "list files successfully" in {
      val storageService = new StubStorageService
      val grpcService = new StubGrpcService
      val handlers = FileManagerHandlers(testApiConfig, storageService, grpcService)

      val request = ListFilesRequest(owner = "test-user", limit = Some(10), cursor = None)
      val result = handlers.listFiles(request).unsafeRunSync()

      result shouldBe a[Right[?, ?]]
      grpcService.listFilesCalled shouldBe true
      val response = result.toOption.get
      response.totalCount shouldBe 0
      response.files shouldBe empty
    }

    "list files with cursor pagination" in {
      val storageService = new StubStorageService
      val grpcService = new StubGrpcService
      val handlers = FileManagerHandlers(testApiConfig, storageService, grpcService)

      val request = ListFilesRequest(owner = "test-user", limit = Some(5), cursor = Some("2"))
      val result = handlers.listFiles(request).unsafeRunSync()

      result shouldBe a[Right[?, ?]]
    }

    "list files with non-numeric cursor defaults to page 0" in {
      val storageService = new StubStorageService
      val grpcService = new StubGrpcService
      val handlers = FileManagerHandlers(testApiConfig, storageService, grpcService)

      val request = ListFilesRequest(owner = "test-user", limit = None, cursor = Some("invalid"))
      val result = handlers.listFiles(request).unsafeRunSync()

      result shouldBe a[Right[?, ?]]
    }

    "download file successfully" in {
      val storageService = new StubStorageService
      val grpcService = new StubGrpcService
      val handlers = FileManagerHandlers(testApiConfig, storageService, grpcService)

      val result = handlers.downloadFile("test-id").unsafeRunSync()

      result shouldBe a[Right[?, ?]]
      grpcService.downloadFileCalled shouldBe true
      val response = result.toOption.get
      response.fileId shouldBe "test-id"
      response.downloadUrl shouldBe "https://storage.example.com/file"
    }

    "return not found when downloading missing file" in {
      val storageService = new StubStorageService
      val grpcService = new StubGrpcService
      grpcService.downloadResponse = GrpcDownloadFileResponse(
        presignedUrl = "",
        s3Key = "",
        status = ResponseStatus.NOT_FOUND,
        errorMessage = "File not found"
      )
      val handlers = FileManagerHandlers(testApiConfig, storageService, grpcService)

      val result = handlers.downloadFile("missing-id").unsafeRunSync()

      result shouldBe a[Left[?, ?]]
      val error = result.swap.toOption.get
      error.error shouldBe "NotFound"
    }

    "return error when download fails with internal error" in {
      val storageService = new StubStorageService
      val grpcService = new StubGrpcService
      grpcService.downloadResponse = GrpcDownloadFileResponse(
        presignedUrl = "",
        s3Key = "",
        status = ResponseStatus.INTERNAL_ERROR,
        errorMessage = "Storage error"
      )
      val handlers = FileManagerHandlers(testApiConfig, storageService, grpcService)

      val result = handlers.downloadFile("test-id").unsafeRunSync()

      result shouldBe a[Left[?, ?]]
      val error = result.swap.toOption.get
      error.error shouldBe "DownloadFailed"
    }

    "handle exception in getFileInfo" in {
      val storageService = new StubStorageService
      val grpcService = new StubGrpcService:
        override def getFileInfo(request: GetFileInfoRequest): Future[GetFileInfoResponse] =
          Future.failed(new RuntimeException("Connection refused"))
      val handlers = FileManagerHandlers(testApiConfig, storageService, grpcService)

      val result = handlers.getFileInfo("test-id").unsafeRunSync()

      result shouldBe a[Left[?, ?]]
      val error = result.swap.toOption.get
      error.error shouldBe "GetInfoFailed"
    }

    "handle exception in deleteFile" in {
      val storageService = new StubStorageService
      val grpcService = new StubGrpcService:
        override def deleteFile(request: DeleteFileRequest): Future[GrpcDeleteFileResponse] =
          Future.failed(new RuntimeException("Connection refused"))
      val handlers = FileManagerHandlers(testApiConfig, storageService, grpcService)

      val result = handlers.deleteFile("test-id").unsafeRunSync()

      result shouldBe a[Left[?, ?]]
      val error = result.swap.toOption.get
      error.error shouldBe "DeleteFailed"
    }

    "handle exception in listFiles" in {
      val storageService = new StubStorageService
      val grpcService = new StubGrpcService:
        override def listFiles(request: GrpcListFilesRequest): Future[GrpcListFilesResponse] =
          Future.failed(new RuntimeException("Connection refused"))
      val handlers = FileManagerHandlers(testApiConfig, storageService, grpcService)

      val request = ListFilesRequest(owner = "user", limit = None, cursor = None)
      val result = handlers.listFiles(request).unsafeRunSync()

      result shouldBe a[Left[?, ?]]
      val error = result.swap.toOption.get
      error.error shouldBe "ListFailed"
    }

    "handle exception in downloadFile" in {
      val storageService = new StubStorageService
      val grpcService = new StubGrpcService:
        override def downloadFile(request: DownloadFileRequest): Future[GrpcDownloadFileResponse] =
          Future.failed(new RuntimeException("Connection refused"))
      val handlers = FileManagerHandlers(testApiConfig, storageService, grpcService)

      val result = handlers.downloadFile("test-id").unsafeRunSync()

      result shouldBe a[Left[?, ?]]
      val error = result.swap.toOption.get
      error.error shouldBe "DownloadFailed"
    }

    "return error when registration fails after upload" in {
      val storageService = new StubStorageService
      val grpcService = new StubGrpcService
      grpcService.registerResponse = RegisterFileResponse(
        fileId = "test-id",
        metadata = None,
        status = ResponseStatus.INTERNAL_ERROR,
        errorMessage = "Actor timeout"
      )
      val handlers = FileManagerHandlers(testApiConfig, storageService, grpcService)

      // Create a temp file for upload testing
      val tempFile = File.createTempFile("test-upload", ".txt")
      tempFile.deleteOnExit()
      java.nio.file.Files.writeString(tempFile.toPath, "test content")

      val request = UploadFileRequest(file = tempFile, owner = "test-user")
      val result = handlers.uploadFile(request).unsafeRunSync()

      result shouldBe a[Left[?, ?]]
      val error = result.swap.toOption.get
      error.error shouldBe "RegistrationFailed"
    }
  }

  "FileManagerHandlers validation" should {

    "reject files exceeding max size" in {
      val smallMaxConfig = testApiConfig.copy(
        validation = testValidationConfig.copy(maxFileSize = 10L)
      )
      val storageService = new StubStorageService
      val grpcService = new StubGrpcService
      val handlers = FileManagerHandlers(smallMaxConfig, storageService, grpcService)

      // Create a temp file larger than 10 bytes
      val tempFile = File.createTempFile("large-file", ".txt")
      tempFile.deleteOnExit()
      java.nio.file.Files.writeString(tempFile.toPath, "this is more than 10 bytes of content")

      val request = UploadFileRequest(file = tempFile, owner = "test-user")
      val result = handlers.uploadFile(request).unsafeRunSync()

      result shouldBe a[Left[?, ?]]
      val error = result.swap.toOption.get
      error.error shouldBe "FileTooLarge"
    }

    "reject files with filenames exceeding max length" in {
      val shortNameConfig = testApiConfig.copy(
        validation = testValidationConfig.copy(maxFilenameLength = 5)
      )
      val storageService = new StubStorageService
      val grpcService = new StubGrpcService
      val handlers = FileManagerHandlers(shortNameConfig, storageService, grpcService)

      // Create a temp file with a long name
      val tempFile = File.createTempFile("this-is-a-very-long-filename", ".txt")
      tempFile.deleteOnExit()
      java.nio.file.Files.writeString(tempFile.toPath, "test")

      val request = UploadFileRequest(file = tempFile, owner = "test-user")
      val result = handlers.uploadFile(request).unsafeRunSync()

      result shouldBe a[Left[?, ?]]
      val error = result.swap.toOption.get
      error.error shouldBe "FilenameTooLong"
    }

    "reject files with disallowed content types" in {
      val restrictedConfig = testApiConfig.copy(
        validation = testValidationConfig.copy(allowedContentTypes = Set("image/jpeg"))
      )
      val storageService = new StubStorageService
      val grpcService = new StubGrpcService
      val handlers = FileManagerHandlers(restrictedConfig, storageService, grpcService)

      // Create a temp .txt file (content type will be text/plain)
      val tempFile = File.createTempFile("test", ".txt")
      tempFile.deleteOnExit()
      java.nio.file.Files.writeString(tempFile.toPath, "test content")

      val request = UploadFileRequest(file = tempFile, owner = "test-user")
      val result = handlers.uploadFile(request).unsafeRunSync()

      // The file may have content type text/plain or application/octet-stream depending on the system
      // If content type is not in the allowed set, it should be rejected
      result shouldBe a[Left[?, ?]]
      val error = result.swap.toOption.get
      error.error shouldBe "InvalidContentType"
    }
  }

  "FileManagerHandlers.apply factory method" should {
    "create a new instance" in {
      val storageService = new StubStorageService
      val grpcService = new StubGrpcService
      val handlers = FileManagerHandlers(testApiConfig, storageService, grpcService)

      handlers should not be null
    }
  }
