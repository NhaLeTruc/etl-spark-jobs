package com.bob.lee.sc.datarepository.impl

import org.apache.spark.sql.DataFrame

import scala.util.Try

class CsvDataStore (
                   storePath: String,
                   selectColumns: List[String],
                   renameColumns: Map[String, String],
                   partitioning: Option[Map[String, String]]
                   ) extends EmptyDataStore with CalypsoSparkSession with LazyLogging {
  /**
   * Add all required DataFrame columns in a list
   * If a requried column does not exist in current DataFrame, select it and assign null to it
   *
   * Rationale: Even though CSV file does not have all the required fields, we still require all columns to be present
   *
   * @param df
   * @return
   */
  private def getConformedColumns(df: DataFrame): Seq[Column] = {
    logger.info(s"Select and map columns in respect to the schema")

    selectColumns.collect{
      // Check if the col exists within the current DataFrame
      case key if Try(df(key)).isSuccess => col(key)
        // scalastyle:off
        // Otherwise, add it to the DataFrame and assign null as value.
      case key => lit(null).as(if (key.contains(".")) key.split('.').last else key)
        // scalastyle:on
    }
  }

  /**
   * Do a select and only select columns that are needed, instead of reading the whole thing.
   *
   * @param dataFrame
   * @return
   */
  protected def doSelect(dataFrame: DataFrame): DataFrame =
    selectColumns.nonEmpty match{
      case true =>
        dataFrame
          .select(getConformedColumns(dataFrame): _*)
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
   * This method reads the CSV file/directory.
   *
   * @return DataFrame
   */
  override def read: DataFrame =
    this.doRename(
      this.doSelect(
        spark.read
          .option("header", true)
          .option("multiLine", true)
          .csv(this.getPartitioningPath)
      )
    )

  /**
   * This function wirtes the DataFrame to a CSV directory. It will create 1 CSV file.
   *
   * @param dataFrame of type DataFrame to write
   */
  override def write(dataFrame: DataFrame): Unit = {
    dataFrame
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", true)
      .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
      .option("quoteAll", true)
      .csv(this.getPartitioningPath)
  }

  /**
   * Companion class which contains come config for the CsvDataStore and a factory to create it.
   */
  object CsvDataStore {

    /**
     * This function is a factory to create the DataStore with the default constructor.
     *
     * @param storePath
     * @param selectColumns
     * @param renameColumns
     * @param partitioning
     * @return
     */
    def apply(storePath: String, selectColums: List[String], renameColumns: Map[String, String], partitioning: Option[Map[String, String]]): CsvDataStore =
      new CsvDataStore(storePath, selectColumns, renameColumns, partitioning)
  }
}
