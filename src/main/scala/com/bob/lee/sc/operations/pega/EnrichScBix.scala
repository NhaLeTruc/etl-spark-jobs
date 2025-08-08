package com.bob.lee.sc.operations.pega

import com.bob.lee.sc.jobs.ScBixReporting.sparkbaseCommandLineOptions
import com.bob.lee.etl.sparkbase.common.PartitionUtils
import com.bob.lee.etl.sparkbase.config.{DataRepositoryConfig, ParquetDataStoreConfig}
import com.bob.lee.etl.sparkbase.datarepository.{DataRepositoryInitializer, DataUID}
import com.bob.lee.etl.sparkbase.datarepository.impl.DataFrameRepository
import com.bob.lee.etl.sparkbase.operations.JobOperation
import com.bob.lee.etl.sparkbase.spark.sparkbaseSparkSession
import com.bob.lee.sc.operations.pega.helpers.DataFrameHelper
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, from_utc_timestamp, to_timestamp, when}
import pureconfig.generic.vnto._

class EnrichScBix (
                  outputDataUID: DataUID,
                  dataRepositoryConfig: DataRepositoryConfig
                  ) extends JobOperation with sparkbaseSparkSession with LazyLogging {
  override def execute: Unit = {
    val conf = sc.hadoopConfiguration

    val storePath = dataRepositoryConfig.dataStoreConfigs.map({
      case config: ParquetDataStoreConfig => config.path
      case _ => null
    }).head

    val partitioning = Option(PartitionUtils.getPartitionsFromDate(sparkbaseCommandLineOptions.runDate))

    val path: String = partitioning match {
      case Some(x) => s"${storePath}/${x.map((partition) => s"${partition._1}=${partition._2}").mkString("/")}"
      case None => storePath
    }

    val files = FileSystem.get(conf).listStatus(new Path(path))
    val fileNames = files.map(_.getPath.getName)

    val inputDf = readFiles(path, fileNames.filter(_.endsWith(".csv")).minBy(_.length))
      .filter(col("Case Type").isin("CDD Onboarding") && col("Customer type").isin("Organisation"))

    // Writing filtered df to a parquet file for controls.
    DataRepositoryInitializer(Option(dataRepositoryConfig), sparkbaseCommandLineOptions.runDate)
    // If heading has " " add underscore
    val filterDf = inputDf.columns.foldLeft(inputDf){
      (memoDF, colName) => memoDF.withColumnRenamed(colName, colName.replaceAll(" ", "_"))
    }

    DataFrameRepository.add("filteredScBix", filterDf)

    val pyWorkPartyDf = readFiles(path, fileNames.filter(_.endsWith("pyWorkParty.csv"))(0))

    val pxFlowDf = readFiles(path, fileNames.filter(_.endsWith("pxFlow.csv"))(0))

    val highRiskReasonDf = readFiles(path, fileNames.filter(_.endsWith("HighRiskReasons.csv"))(0))
      .select(col("pzInsKey"), col("Value").as("reason_for_high_risk"))

    val unacceptableReasonDf = readFiles(path, fileNames.filter(_.endsWith("UnacceptableReasons.csv"))(0))
      .select(col("pzInsKey"), col("Value").as("unacceptable_customer_reasons"))

    val finalDf = renameCols(inputDf)
      .join(pyWorkPartyMap(pyWorkPartyDf), Seq("pzInsKey"), "left")
      .join(pxFlowDf(pxFlowDf), Seq("pzInsKey"), "left")
      .join(highRiskReasonDf, Seq("pzInsKey"), "left")
      .join(unacceptableReasonDf, Seq("pzInsKey"), "left")

    spark.conf.set("spark.sql.session.timeZone", "vntralia/Melbourne")

    DataFrameRepository.add(outputDataUID, finalDf)
  }

  private def readFiles(path: String, pathEnd: String): DataFrame = {
    spark.read
      .option("ignoreLeadingWhiteSpace", true)
      .option("header", true)
      .option("multiLine", true)
      .option("escape", "\"")
      .csv(path + "/" + pathEnd)
  }

  private def pyWorkPartyMap(df: DataFrame): DataFrame = {
    df.filter(col("PARTY SUBSCRIPT") === "Customer")
      .select(
        col("pzInsKey"),
        col("PARTY_NUMBER").as("customer_id"),
        col("ORGANIZATION NAME").as("customer_name"),
        col("LEGAL NAME").as("legal_name_confirmed")
      )
  }

  private def pxFlowMap(df: DataFrame): DataFrame = {
    df.filter(col("Subscript") === "CompleteCDDAssessment")
      .select(
        col("pzInsKey"),
        col("RouteToUserName").as("assigned_to")
      )
  }

  private def renameCols(df: DataFrame): DataFrame = {
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    var renameDf = df
      .withColumnRenamed("XXXXXXXX", "XXX")
      .withColumnRenamed("XXXXXXXX", "XXX")
      .withColumnRenamed("XXXXXXXX", "XXX")
      .withColumnRenamed("XXXXXXXX", "XXX")
      .drop("XXXXXX")
      .withColumnRenamed("XXXXXXXX", "XXX")

    renamedDf = renameDf.columns.foldLeft(renameDf) {
      (memoDF, colName) =>
        memoDF.withColumnRenamed(colName, colName.replaceAll(" ", "_").toLowerCase
    }

    renamedDf
      .withColumnRenamed("pzinskey", "pzInsKey")
      .withColumn("created_on", to_timestamp(col("created_on"), "yyyyMMdd'T'HHmmss"))
      .withColumn("last_updated_on", to_timestamp(col("last_updated_on"), "yyyyMMdd'T'HHmmss"))
      .withColumn("resolved_on", to_timestamp(col("resolved_on"), "yyyyMMdd'T'HHmmss"))
      .drop("customer_id", "assigned_to")
  }
}

object EnrichScBix {
  def apply(outputDataUID: DataUID, dataRepositoryConfig: DataRepositoryConfig): EnrichScBix =
    new EnrichScBix(outputDataUID: DataUID, dataRepositoryConfig: DataRepositoryConfig)
}