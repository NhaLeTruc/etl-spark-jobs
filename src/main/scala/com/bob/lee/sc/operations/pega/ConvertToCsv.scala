package com.bob.lee.sc.operations.pega

import com.bob.lee.sc.config.CsvDataRepositoryConfig
import com.bob.lee.sc.datarepository.impl.CsvDataRepositoryInitializer

// Update line 5 to import ScReporting.sparkbaseCommandLineOptions when reverting to parquet files.

class ConvertToCsv (outputDataUID: DataUID) extends JobOperation {
  override def execute: Unit = {
    val df = DataFrameRepository(outputDataUID)

    // Load the DataRepository configuration
    lazy val otherConfig = sparkbaseConfigLoader("data-repository-config", sparkbaseCommandLineOptions.configFile.get)
      .loadOrThrow[CsvDataRepositoryConfig]

    // Initialise DataRepository based on the DataRepository Configuration
    CsvDataRepositoryInitializer(otherConfig, sparkbaseCommandLineOptions.runDate)

    DataStoreRepository("conformed" -> "sc").write(df)
  }

}

object ConvertToCsv{
  def apply(outputDataUID: DataUID): ConvertToCsv = new ConvertToCsv(outputDataUID)
}
