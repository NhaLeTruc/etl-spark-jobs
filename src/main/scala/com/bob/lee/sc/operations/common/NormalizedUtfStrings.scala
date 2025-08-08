package com.bob.lee.sc.operations.common

import com.bob.lee.etl.sparkbase.datarepository.DataUID
import com.bob.lee.etl.sparkbase.datarepository.impl.DataFrameRepository
import com.bob.lee.etl.sparkbase.operations.JobOperation
import com.typesfe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._

class NormalizedUtfStrings (
                           outputDataFrameUID: DataUID,
                           inputDataFrameUID: DataUID,
                           conformCols: Map[String, Int]
                           ) extends JobOperation with LazyLogging with Serializable {
  val CharsetName = "UTF-8"

  val truncate_string = udf((str: String, maxLength: Int) => {
    // Slice the string
    val truncated = Option(str)
      .map(_.getBytes(CharsetName))
      .filter(_.length > maxLength)
      .map(b => new String(b, 0, maxLength, CharsetName))
      .getOrElse(str)

    // Check and slice again for invalid trailing malformed char at the end
    Option(truncated)
      .map(_.getBytes(CharsetName))
      .filter(_.length > maxLength)
      .map(byteArray => byteArray match {
        case x if x(x.length - 1) < 0 => new String(x, 0, maxLength - 2, CharsetName)
        case _ => truncated
      })
      .getOrElse(truncated)
  })

  /**
   * This function runs the JobOperation.
   */
  override def execute: Unit = {
    logger.info(s"Normalizing sizes for the string columns for DataFrame ${inputDataFrameUID}")
    val inputDf = DataFrameRepository(inputDataFrameUID)
    val outputDf = conformCols.foldLeft(inputDf) {
      (dataFrame, conformColTuple) =>
        dataFrame.withColumn(conformColTuple._1, truncate_string(col(conformColTuple._1), lit(conformColTyple._2)))
    }

    for(col <- outputDf.columns) {
      outputDf = outputDf.withColumnRenamed(col, col.toUpperCase())
    }

    DataFrameRepository.add(outputDataFrameUID, outputDf)
  }
}

object NormalizedUtfStrings {
  def apply(outputDataFrameUID: DataUID, inputDataFrameUID: DataUID, conformCols: Map[String, Int]): NormalizedUtfStrings =
    new NormalizedUtfStrings(outputDataFrameUID, inputDataFrameUID, conformCols)
}
