package com.bob.lee.sc.datarepository.impl

import org.apache.spark.sql.DataFrame

/**
 * This class is the DataStore to write DataFrames to Oracle
 *
 * @param dbUrl
 * @param schemaName
 * @param tableName
 * @param dbUser
 * @param dbPassword
 */

class OracleCountsReaderDataStore(
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
  protected def getJDBCConnectionOptions: Map[String, String] = Map(
    "driver" -> "oracle.jdbc.driver.OracleDriver",
    "url" -> dbUrl,
    "user" -> dbUser,
    "password" -> dbPassword
  )

  override def read: DataFrame = spark.read
    .format("jdbc")
    .options(getJDBCConnectionOptions)
    .option("dbtable", s"(SELECT COUNT(1) FROM ${schemaName}.${tableName}")
    .load
}

object OracleCountsReaderDataStore {
  def apply(dbUrl: String, dbUser: String, dbPassword: String, schemaName: String, tableName: String): OracleCountsReaderDataStore =
    new OracleCountsReaderDataStore(dbUrl, dbUser, dbPassword, schemaName, tableName)
}
