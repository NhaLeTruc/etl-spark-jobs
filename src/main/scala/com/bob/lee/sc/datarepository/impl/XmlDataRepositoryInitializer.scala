package com.bob.lee.sc.datarepository.impl

import com.bob.lee.sc.config.XmlDataRepositoryConfig

import java.time.LocalDate

object XmlDataRepositoryInitializer extends LazyLogging {
  def apply(xmlDataRepositoryConfig: XmlDataRepositoryConfig, runDate: LocalDate): Unit = {
    xmlDataRepositoryConfig.dataStoreConfig.map(dataStoreConfig => {
      val dataUID = dataStoreConfig.dataUID
      val xmlDataStore = XmlDataSTore(
        dataStoreConfig.path,
        dataStoreConfig.rowTag,
        dataStoreConfig.selectColumns.getOrElse(List.empty),
        dataStoreConfig.renameColumns.getOrElse(Map.empty),
        if (dataStoreConfig.partitioning == Some(true)) Some(PartitionUtils.getPartitionsFromDate(runDate))
        else None
      )
      DataStoreRepository.add(dataUID, xmlDataStore)
      DataStoreRepository.add(dataUID, DataStoreRepository(dataUID).read)
    })
  }
}
