package com.bob.lee.sc.jobs

import com.bob.lee.operations.common.NormalizedUtfStrings
import com.bob.lee.etl.sparkbase.sparkbaseApplication
import com.bob.lee.etl.sparkbase.config.sparkbaseConfigLoader
import com.bob.lee.etl.sparkbase.datarepository.impl.DataStoreRepository
import com.bob.lee.etl.sparkbase.operations.impl.{LoadDataStore, NormalizedConform, PersistDataFrame}
import com.bob.lee.sc.config.OtherConfig
import com.bob.lee.sc.datarepository.impl.OracleWriterDataStore
import pureconfig.generic.vnto._

object ScOracleWriteBackJob extends sparkbaseApplication {
  lazy val otherConfig = sparkbaseConfigLoader("other-config", sparkbaseCommandLineOptions.configFile.get)
    .loadOrThrow[OtherConfig]

  val oracleWriterDataStore = OracleWriterDataStore(
    dbUrl = otherConfig.oracleDataStoreConfig.dbUrl,
    dbUser = otherConfig.oracleDataStoreConfig.dbUser,
    dbPassword = otherConfig.oracleDataStoreConfig.dbPassword,
    schemaName = otherConfig.oracleDataStoreConfig.schemaName,
    tableName = otherConfig.oracleDataStoreConfig.tableName
  )
  DataStoreRepository.add(otherConfig.oracleDataStoreConfig.dataUID, oracleWriterDataStore)

  val jobPipeline = LoadDataStore("conformed" -> "sc") >>
    NormalizedConform("conformed" -> "sc", "conformed" -> "sc", otherConfig.conformanceMap) >>
    NormalizedUtfStrings("conformed" -> "sc", "conformed" -> "sc", otherConfig.sizeMap) >>
    PersistDataFrame(otherConfig.oracleDataStoreConfig.dataUID, "conformed" -> "sc") >>
    ControlCompareRowCount("filtered" -> "sc", otherConfig.oracleDataStoreConfig, sparkbaseCommandLineOptions)

  jobPipeline.execute
}
