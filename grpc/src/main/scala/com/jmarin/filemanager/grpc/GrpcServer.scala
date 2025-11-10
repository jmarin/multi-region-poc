package com.jmarin.filemanager.grpc

import com.jmarin.filemanager.persistence.FileManager
import com.jmarin.filemanager.storage.{MinioStorageService, StorageService}
import com.typesafe.config.Config
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity}
import org.apache.pekko.grpc.scaladsl.{ServerReflection, ServiceHandler}
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.{HttpRequest, HttpResponse}
import org.apache.pekko.persistence.typed.ReplicaId
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** gRPC server for FileManager service.
  *
  * This object provides methods to initialize cluster sharding and start the gRPC server using Pekko HTTP/2.
  */
object GrpcServer:
  private val logger = LoggerFactory.getLogger(getClass)

  /** Initialize cluster sharding for FileManager entities.
    *
    * @param system
    *   The actor system
    * @param config
    *   Configuration
    * @param regionId
    *   The region identifier
    */
  def initializeSharding(
      system: ActorSystem[?],
      config: Config,
      regionId: String
  ): Unit =
    val replicaId     = ReplicaId(regionId)
    val allReplicaIds = config
      .getStringList("pekko.persistence.typed.replicated-event-sourcing.replicas")
      .toArray
      .map(_.toString)
      .map(ReplicaId.apply)
      .toSet

    val sharding = ClusterSharding(system)
    sharding.init(Entity(FileManager.TypeKey): entityContext =>
      FileManager(entityContext.entityId, replicaId, allReplicaIds))
    logger.info("Cluster sharding initialized for FileManager entities")

  /** Start the gRPC server.
    *
    * @param system
    *   The actor system
    * @param config
    *   Configuration
    * @param regionId
    *   The region identifier
    * @return
    *   Future containing the server binding
    */
  def start(
      system: ActorSystem[?],
      config: Config,
      regionId: String
  ): Future[Http.ServerBinding] =
    given ActorSystem[?]   = system
    given ExecutionContext = system.executionContext

    // Initialize storage service
    val storageService: StorageService = MinioStorageService(regionId, config)

    // Create service implementation
    val service: FileManagerService = new FileManagerServiceImpl(system, storageService, regionId)

    // Bind service handlers with server reflection
    val serviceHandlers: HttpRequest => Future[HttpResponse] =
      ServiceHandler.concatOrNotFound(
        FileManagerServiceHandler.partial(service),
        ServerReflection.partial(List(FileManagerService))
      )

    // Get server configuration
    val interface = config.getString("pekko.grpc.server.host")
    val port      = config.getInt("pekko.grpc.server.port")

    // Start the HTTP/2 server
    val binding = Http()
      .newServerAt(interface, port)
      .bind(serviceHandlers)

    binding.onComplete:
      case Success(binding) =>
        val address = binding.localAddress
        logger.info(s"gRPC server started at ${address.getHostString}:${address.getPort}")
      case Failure(ex)      =>
        logger.error(s"Failed to bind gRPC server to $interface:$port", ex)
        system.terminate()

    binding
