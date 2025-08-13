"""
Repository for minio-lake related methods
"""

# Externals
from typing import List, Optional
from pyspark.sql import DataFrame

# Internals
from core.utils import get_or_create_spark_session, get_container_endpoint
from core.constants import StorageFormats


def minio_create(
    df: DataFrame,
    path: str,
    partition_cols: Optional[List[str]] = None,
    format_type: StorageFormats = StorageFormats.MINIO_STORAGE_FORMAT,
    mode: str = "overwrite",
    options: dict = {},
) -> None:
    """
    Land Spark DataFrame in minio-lake bucket path

    Args:
        df: Spark DataFrame to be landed.
        path: string of minio-lake landing bucket path.
        format_type: landed file type specificiation.
        mode: landed file writing mode (append; overwrite)
        options: Being explicit about overwriting only the required partitions
    """
    writer = df.write
    if partition_cols:
        writer = writer.partitionby(*partition_cols)
    writer.format(format_type).mode(mode).options(**options).save(path)


def minio_read(
    path: str,
    sql: Optional[str] = None,
    table_name: str = "MINIO",
    format_type: StorageFormats = StorageFormats.MINIO_STORAGE_FORMAT,
) -> DataFrame:
    """
    Read data in minio-lake bucket path as a Spark DataFrame

    Args:
        path: string of minio-lake bucket path.
        sql: SQL query for querying minio-lake's data in path.
        table_name: If sql is provided, use this as name of the queried data.
        format_type: landed file type specificiation.

    Returns:
        minio-lake data as a Spark DataFrame
    """
    spark = get_or_create_spark_session(appname="Minio_read")

    if sql:
        spark.read.format(format_type).load(path).createOrReplaceTempView(table_name)
        return spark.sql(sql)
    return spark.read.format(format_type).load(path)


