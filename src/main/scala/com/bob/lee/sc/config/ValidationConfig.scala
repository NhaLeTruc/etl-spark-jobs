package com.bob.lee.sc.config

import com.bob.lee.odyssey.calypso.config.ParquetDataStoreConfig

case class ValidationConfig(
                           oracleDataStoreConfig: OracleDataStoreConfig,
                           parquetDataStoreConfig: ParquetDataStoreConfig
                           )
