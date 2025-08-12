"""
Repository for minio-lake related methods
"""

# Externals
import os
from typing import List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StructType

# Internals
from core.utils import get_or_create_spark_session
from core.constants import StorageFormats

def get_minio_endpoint() -> str:
    """
    Get url address of MINIO s3 endpoint in dev docker network
    """
    CMD = "curl -v minio-lake:9000 2>&1 | grep -o '(.*).' | tr -d '() '"
    return 'http://' + os.popen(CMD).read().replace('\n', '')  +':9000'


def persist_minio(
    df: DataFrame,
    path: str,
    partition_cols: Optional[List[str]] = None,
    format_type: StorageFormats = StorageFormats.MINIO_STORAGE_FORMAT,
    mode: str = "overwrite",
    options: dict = {},
) -> None:
    """
    Land data as Spark DataFrame in minio-lake bucket in path

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


def read_minio(
    path: str,
    sql: Optional[str] = None,
    table_name: str = "MINIO",
    format_type: StorageFormats = StorageFormats.MINIO_STORAGE_FORMAT,
) -> DataFrame:
    """
    Read data as Spark DataFrame in minio-lake bucket in path

    Args:
        path: string of minio-lake bucket path.
        sql: SQL query for querying minio-lake's data in path.
        table_name: If sql is provided, use this as name of the queried data.
        format_type: landed file type specificiation.

    Returns:
        minio-lake data as a Spark DataFrame
    """
    spark = get_or_create_spark_session(stage_description=f"Reading minio-lake data in: {path}")
    if sql:
        spark.read.format(format_type).load(path).createOrReplaceTempView(table_name)
        return spark.sql(sql)
    return spark.read.format(format_type).load(path)


