"""
Repository of reusable utility methods
"""
# Externals
import os, json, zipfile
from os.path import abspath, dirname, join
from typing import Any, Optional, Dict, List
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

# Internals
from apps.core.conf.minio_config import DockerEnvMinioConfig
from apps.core.conf.storage import DOCKER_ENV
from apps.core.constants import DateTimeFormat


def get_or_create_spark_session(
    appname: str="etl_job",
    configs: Dict={},
    stage_description: Optional[str]=None,
) -> SparkSession:
    """
    Get or create SparkSesion with additional configs other than those in spark-defaults.conf
    """
    spark_builder = SparkSession.builder.appName(appname)

    minio_conf = DockerEnvMinioConfig(
        config=DOCKER_ENV.get("minio-lake")
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


def read_module_file(
    file_path: str,
    caller_path: str=__file__,
) -> str:
    """
    Give a path relative to "core" module by default, read its content and return it.

    Args:
        path: path to file, relative to core directory.

    Returns:
        Contents of file as a string.
    """
    current_path = abspath(dirname(caller_path))
    destination_path = join(current_path, file_path)

    # Handle spark submit static file paths
    if ".zip" in destination_path:
        zip_filename, core_dir = current_path.split(".zip/", 1)
        zip_path = f"{zip_filename}.zip"
        with zipfile.ZipFile(zip_path, "r") as zip_file, zip_file.open(
            f"{core_dir}/{file_path}", "r"
        ) as file:
            return file.read().decode(encoding="utf-8-sig")
 
    with open(destination_path, encoding="utf-8-sig") as file:
        return file.read().replace("\n", " ").rstrip()


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

    if parition_size.days < 1:
        raise ValueError("num_partitions too large for date bounds!")

    partition_from_dt = (from_dt + timedelta(days=parition_size.days)).strftime(date_format)
    partition_to_dt = (to_dt - timedelta(days=parition_size.days)).strftime(date_format)

    if partition_from_dt >= partition_to_dt:
        raise ValueError("Cannot find valid partition dates!")

    return [partition_from_dt, partition_to_dt]

