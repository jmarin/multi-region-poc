package com.jmarin.filemanager.storage

import com.typesafe.config.Config
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Sink, Source, StreamConverters}
import org.apache.pekko.util.ByteString
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.async.{AsyncRequestBody, AsyncResponseTransformer}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.*
import software.amazon.awssdk.services.s3.presigner.S3Presigner
import software.amazon.awssdk.services.s3.presigner.model.{GetObjectPresignRequest, PresignedGetObjectRequest}

import java.net.{URI, URL}
import java.time.Duration
import java.util.concurrent.CompletableFuture
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*
import scala.jdk.FutureConverters.*
import scala.util.{Failure, Success, Try}

/** MinIO/S3-compatible storage service implementation.
  *
  * This implementation uses the AWS SDK S3 client to interact with MinIO, which provides an S3-compatible API. It
  * handles file uploads, downloads, deletions, and presigned URL generation.
  *
  * @param regionId
  *   The region identifier (e.g., "us-east-1")
  * @param config
  *   The configuration object containing S3/MinIO settings
  * @param system
  *   The actor system for materializing streams
  */
class MinioStorageService(
    regionId: String,
    config: Config
)(using system: ActorSystem[?])
    extends StorageService:

  private val logger                       = LoggerFactory.getLogger(getClass)
  private given ec: ExecutionContext       = system.executionContext
  private given materializer: Materializer = Materializer(system)

  // Read configuration
  private val s3Config   = config.getConfig(s"s3.$regionId")
  private val endpoint   = s3Config.getString("endpoint")
  private val bucketName = s3Config.getString("bucket")
  private val accessKey  = s3Config.getString("access-key")
  private val secretKey  = s3Config.getString("secret-key")
  private val region     = s3Config.getString("region")

  logger.info(s"Initializing MinIO storage service for region $regionId with endpoint $endpoint and bucket $bucketName")

  // Create S3 async client
  private val s3Client: S3AsyncClient = S3AsyncClient
    .builder()
    .endpointOverride(URI.create(endpoint))
    .region(Region.of(region))
    .credentialsProvider(
      StaticCredentialsProvider.create(
        AwsBasicCredentials.create(accessKey, secretKey)
      )
    )
    .forcePathStyle(true) // Required for MinIO
    .build()

  // Create S3 presigner for generating presigned URLs
  private val s3Presigner: S3Presigner = S3Presigner
    .builder()
    .endpointOverride(URI.create(endpoint))
    .region(Region.of(region))
    .credentialsProvider(
      StaticCredentialsProvider.create(
        AwsBasicCredentials.create(accessKey, secretKey)
      )
    )
    .build()

  /** Upload a file to MinIO.
    *
    * The file is stored with a key format: {fileId}/{fileName} This allows for easy organization and retrieval.
    */
  override def uploadFile(
      fileId: String,
      fileName: String,
      content: Source[ByteString, Any],
      contentLength: Long,
      contentType: String
  ): Future[String] =
    val s3Key = s"$fileId/$fileName"

    logger.info(s"Uploading file to MinIO: bucket=$bucketName, key=$s3Key, size=$contentLength bytes")

    // Materialize the source to a byte array
    content
      .runWith(Sink.fold(ByteString.empty)(_ ++ _))
      .flatMap: data =>
        val putRequest = PutObjectRequest
          .builder()
          .bucket(bucketName)
          .key(s3Key)
          .contentType(contentType)
          .contentLength(contentLength)
          .build()

        val requestBody = AsyncRequestBody.fromBytes(data.toArray)

        s3Client
          .putObject(putRequest, requestBody)
          .asScala
          .map: response =>
            logger.info(s"Successfully uploaded file to MinIO: key=$s3Key, etag=${response.eTag()}")
            s3Key
          .recover:
            case ex: Exception =>
              logger.error(s"Failed to upload file to MinIO: key=$s3Key", ex)
              throw new RuntimeException(s"Failed to upload file to MinIO: ${ex.getMessage}", ex)

  /** Download a file from MinIO.
    *
    * Returns a stream of bytes that can be consumed by the caller.
    */
  override def downloadFile(s3Key: String): Future[Source[ByteString, Any]] =
    logger.info(s"Downloading file from MinIO: bucket=$bucketName, key=$s3Key")

    val getRequest = GetObjectRequest
      .builder()
      .bucket(bucketName)
      .key(s3Key)
      .build()

    val future = s3Client.getObject(getRequest, AsyncResponseTransformer.toBytes[GetObjectResponse]())

    future.asScala
      .map: response =>
        logger.info(s"Successfully retrieved file from MinIO: key=$s3Key, size=${response.asByteArray().length} bytes")
        Source.single(ByteString(response.asByteArray()))
      .recover:
        case ex: NoSuchKeyException =>
          logger.error(s"File not found in MinIO: key=$s3Key", ex)
          throw new RuntimeException(s"File not found: $s3Key", ex)
        case ex: Exception          =>
          logger.error(s"Failed to download file from MinIO: key=$s3Key", ex)
          throw new RuntimeException(s"Failed to download file from MinIO: ${ex.getMessage}", ex)

  /** Delete a file from MinIO.
    */
  override def deleteFile(s3Key: String): Future[Unit] =
    logger.info(s"Deleting file from MinIO: bucket=$bucketName, key=$s3Key")

    val deleteRequest = DeleteObjectRequest
      .builder()
      .bucket(bucketName)
      .key(s3Key)
      .build()

    s3Client
      .deleteObject(deleteRequest)
      .asScala
      .map: response =>
        logger.info(s"Successfully deleted file from MinIO: key=$s3Key")
        ()
      .recover:
        case ex: Exception =>
          logger.error(s"Failed to delete file from MinIO: key=$s3Key", ex)
          throw new RuntimeException(s"Failed to delete file from MinIO: ${ex.getMessage}", ex)

  /** Generate a presigned URL for temporary access to a file.
    *
    * The URL will be valid for the specified duration.
    */
  override def generatePresignedUrl(s3Key: String, expiration: FiniteDuration): Future[URL] =
    logger.info(
      s"Generating presigned URL for MinIO object: bucket=$bucketName, key=$s3Key, expiration=${expiration.toSeconds}s"
    )

    Future:
      val getRequest = GetObjectRequest
        .builder()
        .bucket(bucketName)
        .key(s3Key)
        .build()

      val presignRequest = GetObjectPresignRequest
        .builder()
        .signatureDuration(Duration.ofSeconds(expiration.toSeconds))
        .getObjectRequest(getRequest)
        .build()

      val presignedRequest: PresignedGetObjectRequest = s3Presigner.presignGetObject(presignRequest)
      val url                                         = presignedRequest.url()

      logger.info(s"Generated presigned URL for MinIO object: key=$s3Key, url=$url")
      url
    .recover:
      case ex: Exception =>
        logger.error(s"Failed to generate presigned URL for MinIO object: key=$s3Key", ex)
        throw new RuntimeException(s"Failed to generate presigned URL: ${ex.getMessage}", ex)

  /** Check if a file exists in MinIO.
    */
  override def fileExists(s3Key: String): Future[Boolean] =
    logger.debug(s"Checking if file exists in MinIO: bucket=$bucketName, key=$s3Key")

    val headRequest = HeadObjectRequest
      .builder()
      .bucket(bucketName)
      .key(s3Key)
      .build()

    s3Client
      .headObject(headRequest)
      .asScala
      .map: response =>
        logger.debug(s"File exists in MinIO: key=$s3Key")
        true
      .recover:
        case _: NoSuchKeyException =>
          logger.debug(s"File does not exist in MinIO: key=$s3Key")
          false
        case ex: Exception         =>
          logger.error(s"Error checking if file exists in MinIO: key=$s3Key", ex)
          false

  /** Get the size of a file in MinIO.
    */
  override def getFileSize(s3Key: String): Future[Long] =
    logger.debug(s"Getting file size from MinIO: bucket=$bucketName, key=$s3Key")

    val headRequest = HeadObjectRequest
      .builder()
      .bucket(bucketName)
      .key(s3Key)
      .build()

    s3Client
      .headObject(headRequest)
      .asScala
      .map: response =>
        val size = response.contentLength().longValue() // Convert Java Long to Scala Long
        logger.debug(s"File size in MinIO: key=$s3Key, size=$size bytes")
        size
      .recover:
        case ex: NoSuchKeyException =>
          logger.error(s"File not found in MinIO: key=$s3Key", ex)
          throw new RuntimeException(s"File not found: $s3Key", ex)
        case ex: Exception          =>
          logger.error(s"Failed to get file size from MinIO: key=$s3Key", ex)
          throw new RuntimeException(s"Failed to get file size from MinIO: ${ex.getMessage}", ex)

  /** Shutdown the S3 client and presigner.
    */
  def shutdown(): Unit =
    logger.info(s"Shutting down MinIO storage service for region $regionId")
    s3Client.close()
    s3Presigner.close()

object MinioStorageService:
  /** Factory method to create a MinioStorageService.
    */
  def apply(regionId: String, config: Config)(using system: ActorSystem[?]): MinioStorageService =
    new MinioStorageService(regionId, config)
