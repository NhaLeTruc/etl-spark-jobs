import os
from pyspark.sql import SparkSession

def get_or_create_spark_session(appname: str) -> SparkSession:
    """
    Get or create SparkSesion with additional configs other than those in spark-defaults.conf
    """
    session = (
        SparkSession.builder.appName(appname)
        .config("spark.sql.catalog.nessie.s3.endpoint", get_minio_endpoint())
        .getOrCreate()        
    )

    return session

def get_minio_endpoint() -> str:
    """
    Get url address of MINIO s3 endpoint
    """
    CMD = "curl -v minio-lake:9000 2>&1 | grep -o '(.*).' | tr -d '() '"
    return 'http://' + os.popen(CMD).read().replace('\n', '')  +':9000'
