package com.bob.lee.sc.datarepository.impl

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SaveMode}

class XmlDataStore (
                   storePath: String,
                   rowTag: String,
                   selectColumns: List[String],
                   renameColumns: Map[String, String],
                   partitioning: Option[Map[String, String]]
                   ) extends EmptyDataStore with sparkbaseSparkSession with LazyLogging {
  /**
   * Add all required DataFrame columns in a list
   * If a required column does not exist in current DataFrame, select it and assign null to it.
   *
   * Rationale: Even through XML file does not have all the required fields, we still require all columns to be present.
   *
   * @param df
   * @return
   */
  private def getConformedColumn(df: DataFrame): Seq[Column] = {
    logger.info(s"Select and map columns in respect to the schema")

    selectColumns.collect {
      // Check if the col exist within the current DataFrame
      case key if Try(df(key)).isSuccess => col(key)
        // scalastyle:off
        // Otherwise, add it to the DataFrame and assign null as value
      case key => lit(null).as(if (key.contains(".")) key.split(".").last else key)
        // scalastyle:on
    }
  }

  /**
   * Do a select and only select columns that needed, instead of reading the whole thing.
   *
   * @param dataFrame
   * @return
   */
  protected def doSelect(dataFrame: DataFrame): DataFrame =
    selectColumns.nonEmpty match {
      case true =>
        dataFrame
          .select(getConformedColumn(dataFrame): _*)
      case _ => dataFrame
    }

  /**
   * This method renames certain columns so as to streamline column names.
   *
   * @param dataFrame
   * @return
   */
  protected def doRename(dataFrame: DataFrame): DataFrame =
    renameColumns.foldLeft(dataFrame)((left, right) => {left.withColumnRenamed(right._2, right._1)})

  /**
   * This function resolves the path that the files need to be stored at.
   *
   * @return
   */
  protected def getPartitioningPath: String = {
    partitioning match {
      case Some(x) => s"${storePath}/${x.map((partition) => s"${partition._1}=${partition._2}").mkString("/")}"
      case None => storePath
    }
  }

  /**
   * This method reads the XML file/directory.
   *
   * @return DataFrame
   */
  override def read: DataFrame =
    this.doRename(
      this.doSelect(
        spark.read.option("rowTag", rowTag).xml(this.getPartitioningPath)
      )
    )

  /**
   * This function writes the DataFrame to a XML directory. It will create 1 XML file.
   *
   * @param dataFrame of type DataFrame to wirte.
   */
  override def write(dataFrame: DataFrame): Unit = {
    dataFrame
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .xml(this.getPartitioningPath)
  }
}

object XmlDataStore {
  def apply (storePath: String, rowTag: String, selectColumns: List[String], renameColumns: Map[String, String], partitioning: Option[Map[String, String]]): XmlDataStore =
    new XmlDataStore(storePath, rowTag, selectColumns, renameColumns, partitioning)
}
