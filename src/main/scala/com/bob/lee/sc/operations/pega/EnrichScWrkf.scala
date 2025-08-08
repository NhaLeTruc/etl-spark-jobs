package com.bob.lee.sc.operations.pega

import com.bob.lee.sc.common.universalwrkf.Constants.FCS_DATAUID
import com.bob.lee.etl.sparkbase.datarepository.DataUID
import com.bob.lee.etl.sparkbase.datarepository.impl.DataFrameRepository
import com.bob.lee.etl.sparkbase.operations.JobOperation
import com.bob.lee.etl.sparkbase.spark.sparkbaseSparkSession
import com.bob.lee.sc.operations.pega.helpers.DataFrameHelper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, when}

class EnrichScWrkf (
                   outputDataUID: DataUID
                   ) extends JobOperation with sparkbaseSparkSession with DataFrameHelper {
  override def execute: Unit = {
    val FcsDf: DataFrame = DataFrameRepository(FCS_DATAUID)
      .filter(col("case_type"))
      .isin("CDD Onboard") && col("customer_type")
      .isin("Organization")

    // in FcsDf if heading has " " replace with underscore
    val filterDf = FcsDf.columns.foldLeft(FcsDf){(memoDf, colName) =>
      memoDf.withColumnRenamed(
        colName,
        colName.replace((" ","_"))
      )
    }

    DataFrameRepository.add("filteredDf", filterDf)

    // scalastyle:off
    // Create simpleCompanies_wrkf DataFrame
    val wrkfDf =
      FcsDf
        .join(createCustomerIdDf(FcsDf), Seq("case_id"), "left")
        .join(createResolvingTaskNameDf(FcsDf), Seq("case_id"), "left")
        .join(createSRCreatedByDf(FcsDf), Seq("case_id"), "left")
        .join(createAcipAttribdf(FcsDf), Seq("case_id"), "left")
        .withColumnRenamed("case_id", "ID")
    // scalastylpe:on

    DataFrameRepository.add(outputDataUID, wrlfDf)
  }

  private def createResolvingTaskNameDf(parentDf: DataFrame): DataFrame = {
    val df = selectChildDataFrame(parentDf, "pxFlow", "case_id")

    df.filter(col("subscript") === "CompleteCDDAssessment")
      .select(
        col("case_id"),
        col("pxRouteToUserName").as("Assigned To (CDD Team Member)")
      )
  }

  private def createCustomerIdDf(parentDf: DataFrame): DataFrame = {
    val df = selectChildDataFrame(parentDf, "pyWorkParty", "case_id")

    df.filter(col("subscript") === "Customer")
      .select(
        col("case_id"),
        col("CustomerID").as("Organization Customer #"),
        col("pyCompany").as("Organization Name"),
        col("BankerID").as("Created By"),
        col("LegalName").as("Legal Name Confirmed")
      )
  }

  private def createSRCreatedByDf(parentDf: DataFrame): DataFrame = {
    val df = selectChildDataFrame(parentDf, "RiskProfile", "case_id").select("case_id", "BankerName")
    val df_id =selectChildDataFrame(parentDf, "pyWorkParty", "case_id").select(col("case_id"), col("BankerID"))

    // join banker ID and name
    var newDf = df_id.join(df, Seq("case_id"))
    newDf = newDf.withColumn("BankerID", when(col("BankerID").isNull, col("BankerName")).otherwise(col("BankerID")))

    // join banker ID and source system
    newDf = newDf.join(parentDf.select("case_id", "SR Sub Type"), Seq("case_id"))
    newDf = newDf.withColumn("BankerID", when(col("BankerID").isNull, col("SR Sub Type")).otherwise(col("BankerID")))

    newDf.select(col("case_id"), col("BankerID").as("SR Created By"))
  }

  private def createGenericDataAttribDf(parentDf: DataFrame): DataFrame = {
    val df = selectChildDataFrame(parentDF, "GenericData", "case_id")

    df.select(
      col("case_id"),
      col("BUID").as("Customer BUID"),
      col("pyNote").as("Comments to Banker"),
      col("InvestigationNote").as("Analyst QA Review Comments"),
      col("EscalationName").as("Escalation"),
      col("OpenDate").as("SR Open Date and Time"),
      col("DueDate").as("SR Modificated Date and Time")
    )
  }

  private def createAcipAttribdf(parentDf: DataFrame): DataFrame = {
    val df = selectChildDataFrame(parentDf, "ACIP", "case_id")
    var messageDf = selectChildDataFrame(selectChildDataFrame(df, "ACIPList", "case_id"), "ACIPErrList", "case_id")
      .select(col("case_id"), col("pyRawMessage"), col("isSelected"))

    messageDf = messageDf
      .filter(!col("pyRawMessage").isin("Not Applicable") && col("isSelected") === true)
      .groupBy("case_id")
      .agg(concat_ws("\n", collect_list("pyRawMessage")))
      .withColumnRenamed("concat_ws(\n, collect_list(pyRawMessage))", "CheckList")

    df.select(
      col("case_id"),
      col("IndustryCode").as("Nature of Business"),
      col("Source").as("COE Source"),
      col("ProductCode").as("If Trust Deed"),
      col("CountryOfIncorporation").as("Country of Establishment")
    ).join(messageDf, Seq("case_id"), "left")
  }

}

object EnrichScWrkf {
  def apply(outputDataUID: DataUID): EnrichScWrkf = new EnrichScWrkf(outputDataUID)
}
