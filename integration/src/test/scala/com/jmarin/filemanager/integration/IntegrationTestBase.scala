package com.jmarin.filemanager.integration

import cats.effect.{IO, Resource}
import cats.effect.unsafe.implicits.global
import com.dimafeng.testcontainers.{GenericContainer, PostgreSQLContainer, MultipleContainers}
import com.dimafeng.testcontainers.scalatest.TestContainersForAll
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.client.Client
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration.*
import java.time.Duration as JavaDuration
import java.util.concurrent.atomic.AtomicReference

/** Base class for integration tests with TestContainers.
  *
  * This automatically starts:
  *   - PostgreSQL container for persistence
  *   - MinIO container for object storage
  *
  * Tests can use the provided HTTP client to interact with services.
  */
abstract class IntegrationTestBase extends AsyncWordSpec with Matchers with TestContainersForAll with BeforeAndAfterAll:

  override type Containers = MultipleContainers

  // Immutable reference to HTTP client and cleanup resource
  private val httpClientResource: AtomicReference[Option[(Client[IO], IO[Unit])]] =
    new AtomicReference(None)

  // Store container references immutably
  private val postgresRef: AtomicReference[Option[PostgreSQLContainer]] = new AtomicReference(None)
  private val minioRef: AtomicReference[Option[GenericContainer]]       = new AtomicReference(None)

  def httpClient: Client[IO] = httpClientResource.get() match
    case Some((cli, _)) => cli
    case None           => throw new IllegalStateException("HTTP client not initialized")

  override def startContainers(): Containers =
    // PostgreSQL container
    val postgresContainer = PostgreSQLContainer(
      dockerImageNameOverride = DockerImageName.parse("postgres:16-alpine"),
      databaseName = "filemanager_test",
      username = "test_user",
      password = "test_pass"
    )

    // MinIO container
    val minioContainer = GenericContainer(
      dockerImage = "minio/minio:latest",
      exposedPorts = Seq(9000),
      command = Seq("server", "/data"),
      env = Map(
        "MINIO_ROOT_USER"     -> "minioadmin",
        "MINIO_ROOT_PASSWORD" -> "minioadmin123"
      ),
      waitStrategy = Wait
        .forHttp("/minio/health/live")
        .forPort(9000)
        .withStartupTimeout(JavaDuration.ofSeconds(60))
    )

    // Store references
    postgresRef.set(Some(postgresContainer))
    minioRef.set(Some(minioContainer))

    val containers = MultipleContainers(postgresContainer, minioContainer)
    containers.start()
    containers

  override def afterContainersStart(containers: Containers): Unit =
    // Initialize HTTP client with cleanup resource
    val clientResource = EmberClientBuilder
      .default[IO]
      .withTimeout(30.seconds)
      .withIdleConnectionTime(30.seconds)
      .build

    val (client, cleanup) = clientResource.allocated.unsafeRunSync()
    httpClientResource.set(Some((client, cleanup)))

  override def afterAll(): Unit =
    httpClientResource
      .get()
      .foreach:
        case (_, cleanup) =>
          cleanup.unsafeRunSync()
    super.afterAll()

  /** Get PostgreSQL JDBC URL from container */
  def getPostgresUrl: String =
    postgresRef
      .get()
      .map(_.jdbcUrl)
      .getOrElse(
        throw new IllegalStateException("PostgreSQL container not initialized")
      )

  /** Get MinIO endpoint from container */
  def getMinioEndpoint: String =
    minioRef
      .get()
      .map: minio =>
        s"http://${minio.host}:${minio.mappedPort(9000)}"
      .getOrElse(
        throw new IllegalStateException("MinIO container not initialized")
      )

  /** MinIO credentials */
  def minioAccessKey: String = "minioadmin"
  def minioSecretKey: String = "minioadmin123"

  /** Helper to convert IO to Future for async tests */
  implicit class IOToFuture[A](io: IO[A]):
    def toFuture: Future[A] = io.unsafeToFuture()
