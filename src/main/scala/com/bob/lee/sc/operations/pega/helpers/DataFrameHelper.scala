package com.bob.lee.sc.operations.pega.helpers

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions.{col, desc_nulls_last, explode, lit, row_number, udf}

import scala.util.Try

trait DataFrameHelper {
// Separate file path from file name + only select the first part of file name
  val fileNameUDF: UserDefinedFunction = udf((input: String) => """[\w]*-[\w]*-[\w]*-[\w]*-[\d]*""".r findFirstIn input.split['/'])

  /**
   * Checks the existence of a column
   * If the column doesn't exist, assign null value to it.
   *
   * @param columnName
   * @return
   */
  def assignColumnValue(columnName: String, df: DataFrame): Column = {
    // scalastyle:off
    if (Try(df(columnName)).isSuccess) col(columnName) else lit(null)
    // scalastyle:on
  }

  /**
   * Filter a column based on the values of another column via partitioning (using a window function)
   *
   * @param dataFrame
   * @param partitionBy
   * @param orderBy
   * @return
   */
  def extractTopRecord(dataFrame: DataFrame, partitionBy: String, orderBy: Column, castRequired: Boolean, castAs: String = ""): DataFrame = {
    if (castRequired) {
      dataFrame.withColumn("row_number", row_number()
        .over(Window.partitionBy(partitionBy).orderBy(orderBy.cast(castAs).desc_nulls_last)))
        .where("row_number = 1")
    } else {
      dataFrame.withColumn("row_number", row_number()
        .over(Window.partitionBy(partitionBy).orderBy(orderBy.desc_nulls_last)))
        .where("row_number = 1")
    }
  }

  /**
   * If parentCol column is of type struct then we select its children
   * Otherwise, if parentCol is of type array then we need to explode it first before selecting its children.
   *
   * @param dataFrame
   * @param parentCol
   * @param key
   * @return
   */
  def selectChildDataFrame(dataFrame: DataFrame, parentCol: String, key: String): DataFrame = {
    // Checking for struct or array type in that column
    val isStructType = dataFrame.select(parentCol).schema.map(_.dataType).head.toString.startsWith("StructType")
    val childrenCols = s"$parentCol.*"

    // If column type is struct then use .* to get the nested columns.
    isStructType match {
      case true => dataFrame.select(key, childrenCols)

      // Spark function explode(e: Column) is used to creates a new row for each element in the given array or map column.
      case false => dataFrame.select(explode(col(parentCol)).as(parentCol), col(key))
        .select(key, childrenCols)
    }
  }
}
