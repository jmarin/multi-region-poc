package com.jmarin.filemanager.persistence

import com.typesafe.config.Config
import org.flywaydb.core.Flyway
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/** Database migration utility using Flyway
  *
  * Handles automatic schema migration on application startup
  */
object DatabaseMigration:

  private val logger = LoggerFactory.getLogger(getClass)

  /** Run Flyway migrations for the configured database
    *
    * @param config
    *   Application configuration containing JDBC settings
    * @return
    *   Success if migrations completed successfully, Failure otherwise
    */
  def migrate(config: Config): Try[Unit] =
    Try:
      val jdbcUrl  = config.getString("slick.db.url")
      val user     = config.getString("slick.db.user")
      val password = config.getString("slick.db.password")

      logger.info(s"Starting Flyway migration for database: $jdbcUrl")

      val flyway = Flyway
        .configure()
        .dataSource(jdbcUrl, user, password)
        .locations("classpath:db/migration")
        .baselineOnMigrate(true)
        .load()

      val result = flyway.migrate()

      logger.info(s"Flyway migration completed successfully. Applied ${result.migrationsExecuted} migrations")
    .recoverWith:
      case ex: Exception =>
        logger.error("Flyway migration failed", ex)
        Failure(ex)

  /** Check if database needs migration
    *
    * @param config
    *   Application configuration containing JDBC settings
    * @return
    *   Number of pending migrations
    */
  def pendingMigrations(config: Config): Try[Int] =
    Try:
      val jdbcUrl  = config.getString("slick.db.url")
      val user     = config.getString("slick.db.user")
      val password = config.getString("slick.db.password")

      val flyway = Flyway
        .configure()
        .dataSource(jdbcUrl, user, password)
        .locations("classpath:db/migration")
        .load()

      val pending = flyway.info().pending()
      pending.length

  /** Clean database (useful for testing) - USE WITH CAUTION
    *
    * @param config
    *   Application configuration containing JDBC settings
    */
  def clean(config: Config): Try[Unit] =
    Try:
      val jdbcUrl  = config.getString("slick.db.url")
      val user     = config.getString("slick.db.user")
      val password = config.getString("slick.db.password")

      logger.warn(s"Cleaning database: $jdbcUrl - ALL DATA WILL BE LOST!")

      val flyway = Flyway
        .configure()
        .dataSource(jdbcUrl, user, password)
        .locations("classpath:db/migration")
        .load()

      flyway.clean()
      logger.info("Database cleaned successfully")
