package com.bob.lee.sc.common

import com.bob.lee.sc.config.ValidationConfig
import com.bob.lee.etl.sparkbase.config.{sparkbaseCommandLineOptions, sparkbaseConfigLoader, DataRepositoryConfig}
import com.bob.lee.etl.sparkbase.datarepository.DataRepositoryInitializer
import com.bob.lee.etl.sparkbase.datarepository.impl.DataStoreRepository
import com.bob.lee.sc.datarepository.impl.OracleCountsReaderDataStore
import pureconfig.generic.vnto._

object ValidationConfigInitializer {
  def apply(sparkbaseCommandLineOptions: sparkbaseCommandLineOptions): Unit={
    lazy val validationConfig = sparkbaseConfigLoader("validation-config", sparkbaseCommandLineOptions.configFile.get)
      .loadOrThrow[ValidationConfig]

    val oracleWriterDataStore = OracleCountsReaderDataStore(
      dbUrl = validationConfig.oracleDataStoreConfig.dbUrl,
      dbUser = validationConfig.oracleDataStoreConfig.dbUser,
      dbPassword = validationConfig.oracleDataStoreConfig.dbPassword,
      schemaName = validationConfig.oracleDataStoreConfig.schemaName,
      tableName = validationConfig.oracleDataStoreConfig.tableName
    )
    DataStoreRepostiory.add(validationConfig.oracleDataStoreConfig.dataUID, oracleWriterDataStore)

    DataRepositoryInitializer(
      Some(DataRepositoryConfig(List(validationConfig.parquestDataStoreConfig))),
      sparkbaseCommandLineOptions.runDate
    )

  }
}
