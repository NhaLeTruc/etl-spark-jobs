"""
pyspark postgres db connector
"""


import inspect
import traceback
from datetime import datetime
from typing import Optional

from core.conf.jdbc import JdbcConfig
from core.utils import get_or_create_spark_session
from pyspark.sql import DataFrame


def ops_read(
    sql_query: str,
    config: JdbcConfig,
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
        "driver": config.driver,
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


def ops_write(
    dbtable: str,
    df: DataFrame,
    config: JdbcConfig,
    mode: str = "append",
    options: dict = {},
) -> None:
    """
    Write data to postgres OPS

    Args:
        dbtable: name of table to be appended (default) in postgres OPS.
        df: Spark Dataframe whose data would append (default) dbtable.
    """

    jdbc_options = {
        "url": config.url,
        "user": config.user,
        "password": config.password,
        "driver": config.driver,
        "dbtable": dbtable,
    }

    jdbc_options |= options

    try:
        df.write.format("jdbc").options(**jdbc_options).mode(mode).save()
    except Exception as e:
        print("An error occurred:", e)
        traceback.print_exc()
