package com.jmarin.filemanager.endpoints

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import sttp.tapir.EndpointIO

class FileManagerEndpointsSpec extends AnyWordSpec with Matchers:

  "FileManagerEndpoints" should {

    "define uploadFile endpoint with correct path" in {
      val endpoint = FileManagerEndpoints.uploadFile
      endpoint.info.name shouldBe Some("uploadFile")
      endpoint.input.toString should include("api")
      endpoint.input.toString should include("v1")
      endpoint.input.toString should include("files")
      endpoint.input.toString should include("upload")
    }

    "define getFileInfo endpoint with fileId path parameter" in {
      val endpoint = FileManagerEndpoints.getFileInfo
      endpoint.info.name shouldBe Some("getFileInfo")
    }

    "define deleteFile endpoint" in {
      val endpoint = FileManagerEndpoints.deleteFile
      endpoint.info.name shouldBe Some("deleteFile")
    }

    "define listFiles endpoint with query parameters" in {
      val endpoint = FileManagerEndpoints.listFiles
      endpoint.info.name shouldBe Some("listFiles")
    }

    "define downloadFile endpoint" in {
      val endpoint = FileManagerEndpoints.downloadFile
      endpoint.info.name shouldBe Some("downloadFile")
    }

    "define health endpoint" in {
      val endpoint = FileManagerEndpoints.health
      endpoint.info.name shouldBe Some("health")
    }

    "include all endpoints in the all list" in {
      FileManagerEndpoints.all should have size 6
    }
  }
