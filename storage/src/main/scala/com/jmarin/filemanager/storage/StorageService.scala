package com.jmarin.filemanager.storage

import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString

import java.net.URL
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

/** Storage service interface for S3-compatible object storage.
  *
  * This trait defines the contract for file storage operations including upload, download, deletion, and presigned URL
  * generation. Implementations should handle connection management, retries, and error handling.
  */
trait StorageService:

  /** Upload a file to object storage.
    *
    * @param fileId
    *   Unique identifier for the file
    * @param fileName
    *   Original file name
    * @param content
    *   Pekko Streams source of file data
    * @param contentLength
    *   Size of the file in bytes
    * @param contentType
    *   MIME type of the file
    * @return
    *   Future containing the S3 object key where the file was stored
    */
  def uploadFile(
      fileId: String,
      fileName: String,
      content: Source[ByteString, Any],
      contentLength: Long,
      contentType: String
  ): Future[String]

  /** Download a file from object storage.
    *
    * @param s3Key
    *   The S3 object key identifying the file
    * @return
    *   Future containing a stream of file data
    */
  def downloadFile(s3Key: String): Future[Source[ByteString, Any]]

  /** Delete a file from object storage.
    *
    * @param s3Key
    *   The S3 object key identifying the file
    * @return
    *   Future that completes when the file is deleted
    */
  def deleteFile(s3Key: String): Future[Unit]

  /** Generate a presigned URL for temporary direct access to a file.
    *
    * @param s3Key
    *   The S3 object key identifying the file
    * @param expiration
    *   Duration for which the URL should remain valid
    * @return
    *   Future containing the presigned URL
    */
  def generatePresignedUrl(s3Key: String, expiration: FiniteDuration): Future[URL]

  /** Check if a file exists in object storage.
    *
    * @param s3Key
    *   The S3 object key identifying the file
    * @return
    *   Future containing true if the file exists, false otherwise
    */
  def fileExists(s3Key: String): Future[Boolean]

  /** Get the size of a file in object storage.
    *
    * @param s3Key
    *   The S3 object key identifying the file
    * @return
    *   Future containing the file size in bytes
    */
  def getFileSize(s3Key: String): Future[Long]
