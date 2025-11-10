package com.jmarin.filemanager.api

import cats.effect.IO
import com.typesafe.config.ConfigFactory
import scala.jdk.CollectionConverters.*

case class HttpConfig(
    host: String,
    port: Int
)

case class ValidationConfig(
    maxFileSize: Long,
    allowedContentTypes: Set[String],
    maxFilenameLength: Int
)

case class ApiConfig(
    http: HttpConfig,
    regionId: String,
    validation: ValidationConfig
)

object ApiConfig:
  def load[F[_]]: IO[ApiConfig] =
    IO:
      val config = ConfigFactory.load()

      val httpConfig = HttpConfig(
        host = config.getString("filemanager.http.interface"),
        port = config.getInt("filemanager.http.port")
      )

      val validationConfig = ValidationConfig(
        maxFileSize = config.getLong("filemanager.validation.max-file-size"),
        allowedContentTypes = config.getStringList("filemanager.validation.allowed-content-types").asScala.toSet,
        maxFilenameLength = config.getInt("filemanager.validation.max-filename-length")
      )

      val regionId = sys.env.getOrElse("REGION_ID", "us-east-1")

      ApiConfig(
        http = httpConfig,
        regionId = regionId,
        validation = validationConfig
      )
