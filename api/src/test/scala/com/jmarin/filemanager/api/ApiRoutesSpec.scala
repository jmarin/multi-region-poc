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

import java.net.URL
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration

class ApiRoutesSpec extends AnyWordSpec with Matchers:

  given ec: ExecutionContext = scala.concurrent.ExecutionContext.global

  private val testConfig = ApiConfig(
    http = HttpConfig(host = "localhost", port = 8080),
    regionId = "us-east-1",
    validation = ValidationConfig(
      maxFileSize = 10 * 1024 * 1024L,
      allowedContentTypes = Set("text/plain"),
      maxFilenameLength = 255
    )
  )

  // Minimal stub implementations
  private val stubStorage = new StorageService:
    override def uploadFile(fileId: String, fileName: String, content: Source[ByteString, Any], contentLength: Long, contentType: String): Future[String] =
      Future.successful(s"$fileId/$fileName")
    override def downloadFile(s3Key: String): Future[Source[ByteString, Any]] =
      Future.successful(Source.single(ByteString("data")))
    override def deleteFile(s3Key: String): Future[Unit] =
      Future.successful(())
    override def generatePresignedUrl(s3Key: String, expiration: FiniteDuration): Future[URL] =
      Future.successful(java.net.URI.create("https://example.com").toURL)
    override def fileExists(s3Key: String): Future[Boolean] =
      Future.successful(true)
    override def getFileSize(s3Key: String): Future[Long] =
      Future.successful(100L)

  private val stubGrpc = new FileManagerService:
    override def registerFile(request: RegisterFileRequest): Future[RegisterFileResponse] =
      Future.successful(RegisterFileResponse(fileId = request.fileId, status = ResponseStatus.SUCCESS))
    override def getFileInfo(request: GetFileInfoRequest): Future[GetFileInfoResponse] =
      Future.successful(GetFileInfoResponse(found = false))
    override def deleteFile(request: DeleteFileRequest): Future[GrpcDeleteFileResponse] =
      Future.successful(GrpcDeleteFileResponse(success = true))
    override def listFiles(request: GrpcListFilesRequest): Future[GrpcListFilesResponse] =
      Future.successful(GrpcListFilesResponse(files = Seq.empty, totalCount = 0))
    override def downloadFile(request: DownloadFileRequest): Future[GrpcDownloadFileResponse] =
      Future.successful(GrpcDownloadFileResponse(presignedUrl = "https://example.com", s3Key = "key", status = ResponseStatus.SUCCESS))

  "ApiRoutes" should {
    "create routes from config and services" in {
      val routes = ApiRoutes.routes(testConfig, stubStorage, stubGrpc)

      routes should not be null
    }

    "create routes that include health endpoint" in {
      val routes = ApiRoutes.routes(testConfig, stubStorage, stubGrpc)

      // Test health endpoint by running a request
      val request = org.http4s.Request[IO](
        method = org.http4s.Method.GET,
        uri = org.http4s.Uri.unsafeFromString("/health")
      )

      val response = routes.run(request).value.unsafeRunSync()

      response shouldBe defined
      response.get.status.code shouldBe 200
    }

    "create routes that return 404 for unknown paths" in {
      val routes = ApiRoutes.routes(testConfig, stubStorage, stubGrpc)

      val request = org.http4s.Request[IO](
        method = org.http4s.Method.GET,
        uri = org.http4s.Uri.unsafeFromString("/nonexistent")
      )

      val response = routes.run(request).value.unsafeRunSync()

      // Either None (route not matched) or some 404 response
      response.forall(_.status.code == 404) || response.isEmpty shouldBe true
    }

    "create routes that handle file info requests" in {
      val routes = ApiRoutes.routes(testConfig, stubStorage, stubGrpc)

      val request = org.http4s.Request[IO](
        method = org.http4s.Method.GET,
        uri = org.http4s.Uri.unsafeFromString("/api/v1/files/test-id")
      )

      val response = routes.run(request).value.unsafeRunSync()

      response shouldBe defined
    }

    "create routes that handle delete requests" in {
      val routes = ApiRoutes.routes(testConfig, stubStorage, stubGrpc)

      val request = org.http4s.Request[IO](
        method = org.http4s.Method.DELETE,
        uri = org.http4s.Uri.unsafeFromString("/api/v1/files/test-id")
      )

      val response = routes.run(request).value.unsafeRunSync()

      response shouldBe defined
    }

    "create routes that handle list files requests" in {
      val routes = ApiRoutes.routes(testConfig, stubStorage, stubGrpc)

      val request = org.http4s.Request[IO](
        method = org.http4s.Method.GET,
        uri = org.http4s.Uri.unsafeFromString("/api/v1/files/list?owner=test")
      )

      val response = routes.run(request).value.unsafeRunSync()

      response shouldBe defined
    }

    "create routes that handle download requests" in {
      val routes = ApiRoutes.routes(testConfig, stubStorage, stubGrpc)

      val request = org.http4s.Request[IO](
        method = org.http4s.Method.GET,
        uri = org.http4s.Uri.unsafeFromString("/api/v1/files/test-id/download")
      )

      val response = routes.run(request).value.unsafeRunSync()

      response shouldBe defined
    }
  }
