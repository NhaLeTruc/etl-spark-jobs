package com.bob.lee.sc.config

import com.bob.lee.etl.sparkbase.datarepository.DataUID

case class OracleDataStoreConfig(
                                dataUID: DataUID,
                                dbUrl: String,
                                dbUser: String,
                                dbPassword: String,
                                schemaName: String,
                                tableName: String
                                )

case class DataComparisonEnvironment(envNAme: String)

case class OtherConfig(
                      conformanceMap: Map[String, String],
                      sizeMap: Map[String, Int],
                      oracleDataStoreConfig: OracleDataStoreConfig,
                      dataComparisonEnvironment: DataComparisonEnvironment
                      )

case class JsonList(name: List[String])
