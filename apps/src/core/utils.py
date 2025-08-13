"""
Repository of reusable utility methods
"""
# Externals
import os, json, csv, re, sys, io
from os.path import abspath, dirname, join
from typing import Any, Optional, Dict, List, Generator
from pyspark.sql import SparkSession

# Internals
from core.conf.minio import OpsMinioConfig


def get_or_create_spark_session(
    appname: str="etl_job",
    configs: dict={},
) -> SparkSession:
    """
    Get or create SparkSesion with additional configs other than those in spark-defaults.conf
    """
    spark_builder = SparkSession.builder.appName(appname)
    
    configs = configs | {
        'spark.sql.catalog.nessie.s3.endpoint': OpsMinioConfig.endpoint,
        'spark.hadoop.fs.s3a.endpoint': OpsMinioConfig.endpoint,
        'spark.hadoop.fs.s3a.access.key': OpsMinioConfig.access_key,
        'spark.hadoop.fs.s3a.secret.key': OpsMinioConfig.secret_key,
    }

    for key, value in configs.items():
        spark_builder = spark_builder.config(key, value)

    session = spark_builder.getOrCreate()

    return session


def get_container_endpoint(conname: str, port: str) -> str:
    """
    Get docker container url from docker dev environment network
    """
    CMD = f"curl -v {conname}:{port} 2>&1 | grep -o '(.*).' | tr -d '() '"
    return f"http://{os.popen(CMD).read().replace('\n', '')}:{port}"


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
