package com.jmarin.filemanager.persistence

import com.typesafe.config.ConfigFactory

/** Simple application to test database migrations
  *
  * Run this to initialize all three regional databases
  */
object MigrationRunner extends App:

  val regions = Seq("us-east-1", "eu-west-1", "ap-south-1")

  regions.foreach: region =>
    println(s"Running migrations for region: $region")

    val config = ConfigFactory.load(region)

    DatabaseMigration.migrate(config) match
      case scala.util.Success(_)  =>
        println(s"✓ Successfully migrated database for $region")
      case scala.util.Failure(ex) =>
        println(s"✗ Failed to migrate database for $region: ${ex.getMessage}")
        ex.printStackTrace()

  println("\nMigration completed for all regions")
