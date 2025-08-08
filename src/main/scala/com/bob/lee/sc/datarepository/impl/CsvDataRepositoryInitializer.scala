package com.bob.lee.sc.datarepository.impl

import com.bob.lee.sc.config.CsvDataRepositoryConfig

import java.time.LocalDate

object CsvDataRepositoryInitializer extends LazyLogging{
  def apply(csvDataRepositoryConfig: CsvDataRepositoryConfig, runDate: LocalDate): Unit = {
    csvDataRepositoryConfig.dataStoreConfigs.map(dataStoreConfig => {
      val dataUID = dataStoreConfig.dataUID
      val csvDataStore = CsvDataStore(
        dataStoreConfig.path,
        dataStoreConfig.selectColumns.getOrElse(List.empty),
        dataStoreConfig.renameColumns.getOrElse(Map.empty),
        if(dataStoreConfig.partitioning == Some(true)) Some(PartitionUtils.getPartitionsFromDate(runDate))
        else None
      )

      DataStoreRepository.add(dataUID, csvDataStore)
    })
  }
}
