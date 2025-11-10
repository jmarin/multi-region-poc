package com.jmarin.filemanager.domain

/** State for the FileManager actor
  *
  * @param metadata
  *   Optional file metadata, None if file doesn't exist
  */
final case class FileManagerState(
    metadata: Option[FileMetadata] = None
) extends CborSerializable:

  def isEmpty: Boolean = metadata.isEmpty

  def exists: Boolean = metadata.isDefined

  def withMetadata(meta: FileMetadata): FileManagerState =
    copy(metadata = Some(meta))

  def withReplicaAdded(region: String): FileManagerState =
    metadata match
      case Some(meta) =>
        copy(metadata = Some(meta.copy(replicas = meta.replicas + region)))
      case None       => this

  def cleared: FileManagerState =
    FileManagerState(None)

object FileManagerState:
  val empty: FileManagerState = FileManagerState(None)
