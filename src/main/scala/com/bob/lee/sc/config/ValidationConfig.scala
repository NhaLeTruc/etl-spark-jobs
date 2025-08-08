package com.bob.lee.sc.config

import com.bob.lee.etl.sparkbase.config.ParquetDataStoreConfig

case class ValidationConfig(
                           oracleDataStoreConfig: OracleDataStoreConfig,
                           parquetDataStoreConfig: ParquetDataStoreConfig
                           )
