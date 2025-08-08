package com.bob.lee.sc.operations.pega

import com.bob.lee.etl.sparkbase.datarepository.{DataRepositoryInitializer, DataUID}
import com.bob.lee.etl.sparkbase.datarepository.impl.{DataFrameRepository, DataStoreRepository}
import com.bob.lee.etl.sparkbase.config.{sparkbaseCommandLineOptions, sparkbaseConfigLoader, DataRepositoryConfig}
import com.bob.lee.etl.sparkbase.operations.JobOperation
import com.bob.lee.etl.sparkbase.spark.sparkbaseSparkSession
import com.bob.lee.sc.config.OracleDataStoreConfig
import com.bob.lee.sc.datarepository.impl.XmlDataRepositoryInitializer

class ControlCompareRowCount (
                             parquetDataUID: DataUID,
                             oracleData: OracleDataStoreConfig,
                             sparkbaseCommandLineOptions: sparkbaseCommandLineOptions
                             ) extends JobOperation with LazyLogging with sparkbaseSparkSession {
  override def execute: Unit = {

    val oracleMap = Map (
      "driver" -> "oracle.jdbc.driver.OracleDriver",
      "url" -> oracleData.dbUrl,
      "user" -> oracleData.dbUser,
      "password" -> oracleData.dbPassword
    )

    val oracleDF = spark.read
      .format("jdbc")
      .options(oracleMap)
      .option("dbtable", s"(SELECT COUNT(DISTINCT CASE_ID) FROM ${oracleData.schemaName}.${oracleData.tableName}")
      .load

    val oracleDFCount = oracleDF.first().getDecimal(0).longValue()

    lazy val otherConfig = sparkbaseConfigLoader("data-repository-config", sparkbaseCommandLineOptions.configFile.get)
      .loadOrThrow[OracleDataStoreConfig]

    DataRepositoryInitializer(Option(otherConfig), sparkbaseCommandLineOptions.runDate)

    val parquetDFCount = DataStoreRepository(parquetDataUID).read.count()

    val countDiff = (oracleDFCount - parquetDFCount).abs

    val results = Array((oracleDFCount, parquetDFCount, countDiff))
    val resultDF = spark.createDataFrame(results).toDF("Oracle", "Parquet", "Difference")

    println("FILE ROW COUNT") // scalastyle:ignore
    println("") // scalastyle:ignore
    resultDF.show()

    DataStoreRepository("comparison" -> "sc_result").write(resultDF)
  }
}

object ControlCompareRowCount {
  def apply(parquetDataUID: DataUID, oracleData: OracleDataStoreConfig, sparkbaseCommandLineOptions: sparkbaseCommandLineOptions): ControlCompareRowCount =
    new ControlCompareRowCount(parquetDataUID, oracleData, sparkbaseCommandLineOptions)
}
