package com.bob.lee.sc.config

import com.bob.lee.etl.sparkbase.datarepository.DataUID

case class CsvDataRepositoryConfig(dataStoreConfigs: List[CsvDataStoreConfig])

case class CsvDataRepositoryConfig {
  dataUID: DataUID,
  path: String,
  selectColumns: Option[List[String]],
  renameColumns: Option[Map[String, String]],
  partitioning: Option[Boolean] = Some(true)
}
