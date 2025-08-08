package com.bob.lee.sc.jobs

import com.bob.lee.operations.common.NormalizedUtfStrings
import com.bob.lee.odyssey.calypso.CalypsoApplication
import com.bob.lee.odyssey.calypso.config.CalypsoConfigLoader
import com.bob.lee.odyssey.calypso.datarepository.impl.DataStoreRepository
import com.bob.lee.odyssey.calypso.operations.impl.{LoadDataStore, NormalizedConform, PersistDataFrame}
import com.bob.lee.sc.config.OtherConfig
import com.bob.lee.sc.datarepository.impl.OracleWriterDataStore
import pureconfig.generic.auto._

object ScOracleWriteBackJob extends CalypsoApplication {
  lazy val otherConfig = CalypsoConfigLoader("other-config", calypsoCommandLineOptions.configFile.get)
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
    ControlCompareRowCount("filtered" -> "sc", otherConfig.oracleDataStoreConfig, calypsoCommandLineOptions)

  jobPipeline.execute
}
