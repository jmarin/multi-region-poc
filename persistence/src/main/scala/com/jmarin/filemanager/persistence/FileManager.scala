package com.jmarin.filemanager.persistence

import com.jmarin.filemanager.domain.*
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.cluster.sharding.typed.scaladsl.EntityTypeKey
import org.apache.pekko.persistence.typed.{PersistenceId, ReplicaId, ReplicationId}
import org.apache.pekko.persistence.typed.scaladsl.{
  Effect,
  EventSourcedBehavior,
  ReplicatedEventSourcing,
  ReplicationContext
}

import java.time.Instant

/** FileManager actor with Replicated Event Sourcing
  *
  * This actor manages file metadata and uses Replicated Event Sourcing to ensure consistency across multiple
  * datacenters.
  */
object FileManager:

  val TypeKey: EntityTypeKey[Command] =
    EntityTypeKey[Command]("FileManager")

  def apply(
      entityId: String,
      replicaId: ReplicaId,
      allReplicaIds: Set[ReplicaId]
  ): Behavior[Command] =
    ReplicatedEventSourcing.commonJournalConfig(
      replicationId = ReplicationId("FileManager", entityId, replicaId),
      allReplicaIds = allReplicaIds,
      queryPluginId = "jdbc-read-journal"
    ): replicationContext =>
      EventSourcedBehavior[Command, Event, FileManagerState](
        persistenceId = PersistenceId(TypeKey.name, entityId),
        emptyState = FileManagerState.empty,
        commandHandler = commandHandler,
        eventHandler = eventHandler
      )

  private def commandHandler(
      state: FileManagerState,
      command: Command
  ): Effect[Event, FileManagerState] =
    command match
      case cmd: Command.UploadFile =>
        handleUploadFile(state, cmd)

      case cmd: Command.DownloadFile =>
        handleDownloadFile(state, cmd)

      case cmd: Command.DeleteFile =>
        handleDeleteFile(state, cmd)

      case cmd: Command.GetFileInfo =>
        handleGetFileInfo(state, cmd)

  private def handleUploadFile(
      state: FileManagerState,
      cmd: Command.UploadFile
  ): Effect[Event, FileManagerState] =
    if state.exists then
      // File already exists, just add the region to replicas
      Effect
        .persist(
          Event.FileReplicated(
            fileId = cmd.fileId,
            sourceRegion = state.metadata.get.replicas.head,
            targetRegion = cmd.region,
            replicatedAt = Instant.now()
          )
        )
        .thenRun: _ =>
          val updatedMetadata = state.metadata.get.copy(
            replicas = state.metadata.get.replicas + cmd.region
          )
          cmd.replyTo ! Command.UploadResponse.Success(updatedMetadata)
    else
      // New file upload
      Effect
        .persist(
          Event.FileUploaded(
            fileId = cmd.fileId,
            fileName = cmd.fileName,
            fileSize = cmd.fileSize,
            contentType = cmd.contentType,
            checksum = cmd.checksum,
            region = cmd.region,
            uploadedAt = Instant.now()
          )
        )
        .thenRun: updatedState =>
          updatedState.metadata match
            case Some(meta) =>
              cmd.replyTo ! Command.UploadResponse.Success(meta)
            case None       =>
              cmd.replyTo ! Command.UploadResponse.Failure("Failed to persist file metadata")

  private def handleDownloadFile(
      state: FileManagerState,
      cmd: Command.DownloadFile
  ): Effect[Event, FileManagerState] =
    state.metadata match
      case Some(meta) =>
        Effect
          .persist(
            Event.FileDownloaded(
              fileId = cmd.fileId,
              region = meta.replicas.headOption.getOrElse("unknown"),
              downloadedAt = Instant.now()
            )
          )
          .thenRun: _ =>
            cmd.replyTo ! Command.DownloadResponse.Success(meta)
      case None       =>
        Effect.none.thenRun: _ =>
          cmd.replyTo ! Command.DownloadResponse.NotFound(cmd.fileId)

  private def handleDeleteFile(
      state: FileManagerState,
      cmd: Command.DeleteFile
  ): Effect[Event, FileManagerState] =
    state.metadata match
      case Some(_) =>
        Effect
          .persist(
            Event.FileDeleted(
              fileId = cmd.fileId,
              deletedAt = Instant.now()
            )
          )
          .thenRun: _ =>
            cmd.replyTo ! Command.DeleteResponse.Success(cmd.fileId)
      case None    =>
        Effect.none.thenRun: _ =>
          cmd.replyTo ! Command.DeleteResponse.NotFound(cmd.fileId)

  private def handleGetFileInfo(
      state: FileManagerState,
      cmd: Command.GetFileInfo
  ): Effect[Event, FileManagerState] =
    Effect.none.thenRun: _ =>
      state.metadata match
        case Some(meta) =>
          cmd.replyTo ! Command.FileInfoResponse.Success(meta)
        case None       =>
          cmd.replyTo ! Command.FileInfoResponse.NotFound(cmd.fileId)

  private def eventHandler(
      state: FileManagerState,
      event: Event
  ): FileManagerState =
    event match
      case Event.FileUploaded(fileId, fileName, fileSize, contentType, checksum, region, uploadedAt) =>
        val metadata = FileMetadata(
          fileId = fileId,
          fileName = fileName,
          fileSize = fileSize,
          contentType = contentType,
          uploadedAt = uploadedAt,
          checksum = checksum,
          replicas = Set(region)
        )
        state.withMetadata(metadata)

      case Event.FileDownloaded(_, _, _) =>
        // Download doesn't change state, just logged for auditing
        state

      case Event.FileDeleted(_, _) =>
        state.cleared

      case Event.FileReplicated(_, _, targetRegion, _) =>
        state.withReplicaAdded(targetRegion)
