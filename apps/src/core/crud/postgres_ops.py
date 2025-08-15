"""
pyspark postgres db connector
"""

# Externals
import inspect
from datetime import datetime
from typing import Optional
from pyspark.sql import DataFrame

# Internals
from conf.jdbc import OpsJdbcConfig
from conf.storage import DOCKER_ENV
from utils import get_or_create_spark_session

# OpsJdbcConfig instantiation
# TODO: Where best to do this instantiation??
pg_ops_host = DOCKER_ENV['postgres']['container_name']
pg_ops_port = DOCKER_ENV['postgres']['container_port']
pg_driver = DOCKER_ENV['postgres']['driver']
config = OpsJdbcConfig(
    host_name=pg_ops_host,
    host_port=pg_ops_port,
)

# TODO: Should this be for all type of jdbc spark read?
def read_pg_ops(
    sql_query: str,
    parallel: bool = False,
    partition_column: str = "CREATED_DATE",
    lower_bound: Optional[str] = None,
    upper_bound: Optional[str] = None,
    num_partitions: int = 10,
    fetch_size: int = 10_000,
) -> DataFrame:
    """
    Run sql_query through OPS postgres database optionally in parallel processes.

    Args:
        sql_query_path: Path to a custom READ ONLY SQL query file. Note that dbtable and query cannot be used simultaneously.
        parallel: bool = False,
        partition_column: The name of a numeric column to use for partitioning the data when reading in parallel. Requires lowerBound and upperBound to be specified.
        lower_bound: The lower bound of the partition_column for partitioning. Used only for defining the partitioning strategy and not for filtering the rows.
        upper_bound: The upper bound of the partition_column for partitioning. Used only for defining the partitioning strategy and not for filtering the rows.
        num_partitions:  The maximum number of partitions to use for parallel reading. This determines the maximum number of concurrent JDBC connections.
        fetch_size: The number of rows to fetch from the database at a time.

    Returns:
        A Spark DataFrame from querying the OPS database
    """
    dbtable = f"({sql_query}) as mytable"

    jdbc_options = {
        "url": config.url,
        "user": config.user,
        "password": config.password,
        "driver": pg_driver,
        "dbtable": dbtable,
        "fetchsize": fetch_size
    }

    if parallel:
        if not upper_bound and partition_column == "CREATED_DATE":
            upper_bound = datetime.today().strftime("%Y-%m-%d")

        if not lower_bound or not upper_bound:
            raise ValueError(
                "In parallel mode, lower_bound and upper_bound must be set"
            )
        
        jdbc_options.update(
            {
                "partitionColumn": partition_column,
                "lowerBound": lower_bound,
                "upperBound": upper_bound,
                "numPartitions": num_partitions,
            }
        )
    try:
        caller_name = inspect.stack()[1][3]
    except Exception:
        caller_name = "func_not_found"
    spark = get_or_create_spark_session(
        appname=f"Read OPS data: {caller_name}"
    )
    return spark.read.format("jdbc").options(**jdbc_options).load()


