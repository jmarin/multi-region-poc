package com.jmarin.filemanager.domain

import java.time.Instant

/** File metadata representation
  *
  * @param fileId
  *   Unique identifier for the file
  * @param fileName
  *   Original file name
  * @param fileSize
  *   Size in bytes
  * @param contentType
  *   MIME type
  * @param uploadedAt
  *   Timestamp of upload
  * @param checksum
  *   SHA-256 checksum for integrity verification
  * @param replicas
  *   Set of regions where the file is replicated
  */
case class FileMetadata(
    fileId: String,
    fileName: String,
    fileSize: Long,
    contentType: String,
    uploadedAt: Instant,
    checksum: String,
    replicas: Set[String] = Set.empty
) extends CborSerializable

object FileMetadata:
  def create(
      fileId: String,
      fileName: String,
      fileSize: Long,
      contentType: String,
      checksum: String,
      region: String
  ): FileMetadata =
    FileMetadata(
      fileId = fileId,
      fileName = fileName,
      fileSize = fileSize,
      contentType = contentType,
      uploadedAt = Instant.now(),
      checksum = checksum,
      replicas = Set(region)
    )
