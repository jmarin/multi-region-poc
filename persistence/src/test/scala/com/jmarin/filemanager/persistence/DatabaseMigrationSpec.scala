package com.jmarin.filemanager.persistence

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import java.sql.DriverManager
import scala.util.{Success, Using}

class DatabaseMigrationSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll:

  // Test configuration with in-memory H2 database would be ideal,
  // but for now we'll just test the migration logic without actual DB

  "DatabaseMigration" should {

    "load migration files from resources" in {
      val migrationV1 = getClass.getResourceAsStream("/db/migration/V1__create_journal_table.sql")
      val migrationV2 = getClass.getResourceAsStream("/db/migration/V2__create_snapshot_table.sql")
      val migrationV3 = getClass.getResourceAsStream("/db/migration/V3__create_event_tag_table.sql")

      migrationV1 should not be null
      migrationV2 should not be null
      migrationV3 should not be null

      migrationV1.close()
      migrationV2.close()
      migrationV3.close()
    }

    "parse database configuration from config" in {
      val config = ConfigFactory.parseString("""
        pekko-persistence-jdbc {
          shared-databases {
            default {
              profile {
                db {
                  url = "jdbc:postgresql://localhost:5432/test"
                  user = "testuser"
                  password = "testpass"
                }
              }
            }
          }
        }
      """)

      val url      = config.getString("pekko-persistence-jdbc.shared-databases.default.profile.db.url")
      val user     = config.getString("pekko-persistence-jdbc.shared-databases.default.profile.db.user")
      val password = config.getString("pekko-persistence-jdbc.shared-databases.default.profile.db.password")

      url shouldBe "jdbc:postgresql://localhost:5432/test"
      user shouldBe "testuser"
      password shouldBe "testpass"
    }
  }
