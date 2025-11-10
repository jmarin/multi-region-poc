package com.jmarin.filemanager.domain

import java.time.Instant

/** Events for the FileManager actor */
sealed trait Event extends CborSerializable

object Event:
  /** File was successfully uploaded
    *
    * @param fileId
    *   Unique identifier for the file
    * @param fileName
    *   Original file name
    * @param fileSize
    *   Size in bytes
    * @param contentType
    *   MIME type
    * @param checksum
    *   SHA-256 checksum
    * @param region
    *   Region where file was uploaded
    * @param uploadedAt
    *   Timestamp of upload
    */
  final case class FileUploaded(
      fileId: String,
      fileName: String,
      fileSize: Long,
      contentType: String,
      checksum: String,
      region: String,
      uploadedAt: Instant
  ) extends Event

  /** File was downloaded
    *
    * @param fileId
    *   Unique identifier for the file
    * @param region
    *   Region where file was downloaded from
    * @param downloadedAt
    *   Timestamp of download
    */
  final case class FileDownloaded(
      fileId: String,
      region: String,
      downloadedAt: Instant
  ) extends Event

  /** File was deleted
    *
    * @param fileId
    *   Unique identifier for the file
    * @param deletedAt
    *   Timestamp of deletion
    */
  final case class FileDeleted(
      fileId: String,
      deletedAt: Instant
  ) extends Event

  /** File was replicated to another region
    *
    * @param fileId
    *   Unique identifier for the file
    * @param sourceRegion
    *   Region where file was replicated from
    * @param targetRegion
    *   Region where file was replicated to
    * @param replicatedAt
    *   Timestamp of replication
    */
  final case class FileReplicated(
      fileId: String,
      sourceRegion: String,
      targetRegion: String,
      replicatedAt: Instant
  ) extends Event
