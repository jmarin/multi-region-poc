package com.jmarin.filemanager.api

import cats.effect.unsafe.implicits.global
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

class ApiConfigSpec extends AnyWordSpec with Matchers:

  "HttpConfig" should {
    "store host and port" in {
      val config = HttpConfig(host = "0.0.0.0", port = 8080)

      config.host shouldBe "0.0.0.0"
      config.port shouldBe 8080
    }
  }

  "ValidationConfig" should {
    "store validation parameters" in {
      val config = ValidationConfig(
        maxFileSize = 104857600L,
        allowedContentTypes = Set("text/plain", "application/pdf"),
        maxFilenameLength = 255
      )

      config.maxFileSize shouldBe 104857600L
      config.allowedContentTypes should contain("text/plain")
      config.allowedContentTypes should contain("application/pdf")
      config.maxFilenameLength shouldBe 255
    }

    "handle empty allowed content types" in {
      val config = ValidationConfig(
        maxFileSize = 100L,
        allowedContentTypes = Set.empty,
        maxFilenameLength = 100
      )

      config.allowedContentTypes shouldBe empty
    }
  }

  "ApiConfig" should {
    "compose HttpConfig, regionId, and ValidationConfig" in {
      val httpConfig = HttpConfig(host = "localhost", port = 9090)
      val validationConfig = ValidationConfig(
        maxFileSize = 1000L,
        allowedContentTypes = Set("text/plain"),
        maxFilenameLength = 100
      )
      val apiConfig = ApiConfig(
        http = httpConfig,
        regionId = "us-east-1",
        validation = validationConfig
      )

      apiConfig.http.host shouldBe "localhost"
      apiConfig.http.port shouldBe 9090
      apiConfig.regionId shouldBe "us-east-1"
      apiConfig.validation.maxFileSize shouldBe 1000L
    }

    "load configuration from application.conf" in {
      val config = ApiConfig.load[cats.effect.IO].unsafeRunSync()

      config.http.host shouldBe "0.0.0.0"
      config.http.port shouldBe 8080
      config.validation.maxFileSize shouldBe 104857600L
      config.validation.maxFilenameLength shouldBe 255
      config.validation.allowedContentTypes should not be empty
      config.regionId should not be empty
    }
  }
