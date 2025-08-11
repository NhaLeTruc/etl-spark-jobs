"""
Repository of reusable utility methods
"""

import os
from pyspark.sql import SparkSession

def get_or_create_spark_session(appname: str) -> SparkSession:
    """
    Get or create SparkSesion with additional configs other than those in spark-defaults.conf
    """
    session = (
        SparkSession.builder.appName(appname)
        # Set config e.g.
        # .config("spark.sql.catalog.nessie.s3.endpoint", 'http://minio-lake:9000')
        .getOrCreate()        
    )

    return session
