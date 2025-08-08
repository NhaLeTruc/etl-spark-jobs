package com.bob.lee.sc.common

import com.bob.lee.sc.config.ValidationConfig
import com.bob.lee.odyssey.calypso.config.{CalypsoCommandLineOptions, CalypsoConfigLoader, DataRepositoryConfig}
import com.bob.lee.odyssey.calypso.datarepository.DataRepositoryInitializer
import com.bob.lee.odyssey.calypso.datarepository.impl.DataStoreRepository
import com.bob.lee.sc.datarepository.impl.OracleCountsReaderDataStore
import pureconfig.generic.auto._

object ValidationConfigInitializer {
  def apply(calypsoCommandLineOptions: CalypsoCommandLineOptions): Unit={
    lazy val validationConfig = CalypsoConfigLoader("validation-config", calypsoCommandLineOptions.configFile.get)
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
      calypsoCommandLineOptions.runDate
    )

  }
}
