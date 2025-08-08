package com.bob.lee.sc.datarepository.impl

import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * This class is the DataStore to write DataFrames to Oracle.
 *
 * @param dbUrl
 * @param schemaName
 * @param tableName
 * @param dbUser
 * @param dbPassword
 */
class OracleWriterDataStore (
                            dbUrl: String,
                            dbUser: String,
                            dbPassword: String,
                            schemaName: String,
                            tableName: String
                            ) extends EmptyDataStore with sparkbaseSparkSession with LazyLogging {
  /**
   * This function configures settings for the OracleConnection
   *
   * @return Map of the configurations
   */
  protected def getJDBCConnectionOptions: Map[String, String] = Map (
    "driver" -> "oracle.jdbc.driver.OracleDriver",
    "url" -> dbUrl,
    "user" -> dbUser,
    "password" -> dbPassword
  )

  /**
   * This function configures settings for the Idempotency
   *
   * @return Map of the configurations
   */
  protected def getSessionInitOptions(): Map[String, String] = Map (
    "sessionInitStatement" -> s"TRUNCATE TABLE ${tableName}",
    "dbtable" -> s"(SELECT count(*) AS TOTAL_ROWS FROM ${tableName}) tmp"
  )

  /**
   * This function performs the indempotency operation. Based on a DateColumn, if records already exists,
   * then they are dropped and reinserted. This is done in order to maintain idempotency.
   */
  protected def cleanUpPartition: Unit = {
    val readOptions = getJDBCConnectionOptions ++ getSessionInitOptions()
    logger.info(s"Executing cleanUpPartition")
    spark.read
      .format("jdbc")
      .options(readOptions)
      .load
      .collect
  }

  /**
   * Override this method in order to configure the write methodology.
   *
   * @param dataFrame of type DataFrame to write.
   */
  override def write(dataFrame: DataFrame): Unit = {
    this.cleanUpPartition

    dataFrame.write
      .format("jdbc")
      .mode(SaveMode.Append)
      .options(getJDBCConnectionOptions)
      .option("dbtable", tableName)
      .save()
  }

  override def read: DataFrame = spark.read
    .format("jdbc")
    .options(getJDBCConnectionOptions)
    .option("dbtable", s"(SELECT COUNT(1) FROM ${schemaName}.${tableName})")
    .load
}

/**
 * Companion Object for the OracleWriterDataStore
 */
object OracleWriterDataStore {
  /**
   * Factory that creates the DataStore without Idempotency functionality
   *
   * @param dbUrl
   * @param dbUser
   * @param dbPassword
   * @param schemaName
   * @param tableName
   * @return
   */
  def apply(dbUrl: String, dbUser: String, dbPassword: String, schemaName: String, tableName: String): OracleWriterDataStore =
    new OracleWriterDataStore(dbUrl, dbUser, dbPassword, schemaName, tableName)
}
