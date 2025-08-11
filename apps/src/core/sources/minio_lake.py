"""
Repository for minio-lake related methods
"""

# Externals
import os
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


def fetch_minio() -> DataFrame:
    """
    
    """


def persist_minio(
    df: DataFrame,
    path: str,
    partition_cols: str,
    format_type: StorageFormats,
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
    return
