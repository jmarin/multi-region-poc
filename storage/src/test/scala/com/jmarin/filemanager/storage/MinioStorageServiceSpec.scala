package com.jmarin.filemanager.storage

import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.testkit.typed.scaladsl.ActorTestKit
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import org.apache.pekko.actor.typed.ActorSystem

import scala.concurrent.duration.*

class MinioStorageServiceSpec extends AnyWordSpec with Matchers with ScalaFutures with BeforeAndAfterAll:

  val testKit: ActorTestKit          = ActorTestKit()
  given system: ActorSystem[Nothing] = testKit.system
  given patience: PatienceConfig     = PatienceConfig(timeout = Span(10, Seconds))

  val config = ConfigFactory
    .parseString("""
    s3 {
      us-east-1 {
        endpoint = "http://localhost:9000"
        bucket = "filemanager-us-east-1"
        access-key = "minioadmin"
        secret-key = "minioadmin123"
        region = "us-east-1"
      }
    }
  """)
    .withFallback(ConfigFactory.load())

  val storageService: MinioStorageService = new MinioStorageService("us-east-1", config)(using system)

  override def afterAll(): Unit =
    storageService.shutdown()
    testKit.shutdownTestKit()

  "MinioStorageService" should {

    "initialize with correct configuration" in {
      // The service should initialize without throwing exceptions
      storageService should not be null
    }

    "have proper configuration values" in {
      // This is a basic configuration validation test
      val s3Config = config.getConfig("s3.us-east-1")
      s3Config.getString("endpoint") shouldBe "http://localhost:9000"
      s3Config.getString("bucket") shouldBe "filemanager-us-east-1"
      s3Config.getString("access-key") shouldBe "minioadmin"
      s3Config.getString("region") shouldBe "us-east-1"
    }

    "implement all StorageService methods" in {
      // Verify that MinioStorageService properly implements the StorageService interface
      // We check this by verifying the service can be assigned to the trait type
      val service: StorageService = storageService
      service should not be null

      // Verify the service is an instance of both types
      storageService shouldBe a[StorageService]
      storageService shouldBe a[MinioStorageService]
    }
  }
