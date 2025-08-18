"""
Repository of reusable utility methods
"""
# Externals
import os, json
from os.path import abspath, dirname, join
from typing import Any, Optional, Dict, List
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

# Internals
from apps.core.conf.minio_config import OpsMinioConfig
from apps.core.conf.storage import DOCKER_ENV
from apps.core.constants import DateTimeFormat


def get_or_create_spark_session(
    appname: str="etl_job",
    configs: dict={},
    stage_description: Optional[str]=None,
) -> SparkSession:
    """
    Get or create SparkSesion with additional configs other than those in spark-defaults.conf
    """
    spark_builder = SparkSession.builder.appName(appname)

    # OpsMinioConfig instantiation
    minio_host = DOCKER_ENV['minio-lake']['container_name']
    minio_port = DOCKER_ENV['minio-lake']['container_port']
    minio_conf = OpsMinioConfig(
        host_name=minio_host,
        host_port=minio_port,
    )
        
    configs = configs | {
        'spark.sql.catalog.nessie.s3.endpoint': minio_conf.endpoint,
        'spark.hadoop.fs.s3a.endpoint': minio_conf.endpoint,
        'spark.hadoop.fs.s3a.access.key': minio_conf.access_key,
        'spark.hadoop.fs.s3a.secret.key': minio_conf.secret_key,
    }

    for key, value in configs.items():
        spark_builder = spark_builder.config(key, value)

    session = spark_builder.getOrCreate()

    if stage_description:
        session.sparkContext.setLocalProperty("callSite.short", stage_description)
    return session


def get_container_endpoint(conname: str, port: str) -> str:
    """
    Get docker container url from docker dev environment network
    """
    CMD = f"curl -v {conname}:{port} 2>&1 | grep -o '(.*).' | tr -d '() '"
    return "http://" + os.popen(CMD).read().replace('\n', '') + ":" + {port}


def read_file_content(path: str) -> str:
    """
    Give a path relative to core module, read its content and return it.

    Args:
        path: path to file, relative to core directory.

    Returns:
        Contents of file as a string.
    """
    current_path = abspath(dirname(__file__))
    destination_path = join(current_path, path)

    with open(destination_path, encoding="utf-8-sig") as file:
        return file.read()
    

def read_json_config(path: str) -> Any:
    """
    Read content of JSON file in path relative to core directory and return it.

    Args:
        path: path to file, relative to core directory.

    Returns:
        JSON dictionary
    """
    return json.loads(read_file_content(path))

# TODO: handle 1 day load, wrong inputs
def cal_partition_dt(
    from_dt: str,
    to_dt: str,
    num_partitions: int,
    date_format: str = DateTimeFormat.ISO_DATE_FMT.value
) -> List[str]:
    """
    Suggest partition dates for Spark parallel operations.

    Args:
        from_dt: filter date base on business need.
        to_dt: filter date base on business nedd.
        num_partitions: number of desired Spark parallel operations.
        data_format: IO date format as string.
    Return:
        List of two partition dates: [partition_from_dt, partition_to_dt]
    """
    to_dt = datetime.strptime(to_dt, date_format)
    from_dt = datetime.strptime(from_dt, date_format)

    gap_days = to_dt - from_dt
    parition_size = gap_days / num_partitions

    partition_from_dt = (from_dt + timedelta(days=parition_size.days)).strftime(date_format)
    partition_to_dt = (to_dt - timedelta(days=parition_size.days)).strftime(date_format)

    return [partition_from_dt, partition_to_dt]

