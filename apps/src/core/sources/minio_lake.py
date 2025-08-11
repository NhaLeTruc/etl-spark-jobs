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

def get_minio_endpoint() -> str:
    """
    Get url address of MINIO s3 endpoint in dev docker network
    """
    CMD = "curl -v minio-lake:9000 2>&1 | grep -o '(.*).' | tr -d '() '"
    return 'http://' + os.popen(CMD).read().replace('\n', '')  +':9000'


def fetch_minio() -> DataFrame:
    """
    
    """


def persist_minio() -> None:
    """
    
    """
