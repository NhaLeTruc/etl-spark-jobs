# Externals
from typing import Tuple
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat, lit, sha2, struct

# Internals
from core.conf.jdbc import get_postgres_env
from core.crud.postgres_ops import read_pg_ops
from core.crud.minio_lake import minio_read
from core.mappings.oltp_to_olap_labels import dwh_to_cap_mappings
from core.utils import read_file


def extracts_bronze_transactions(
    from_dt: str,
    to_dt: str,
    partition_column: str,
    num_partitions: int = 10,
    parallel: bool = True,
    fetch_size: int = 10_000,
) -> DataFrame:
    """
    Run SQL script to fetch postgres ops transactions.

    Args:
        from_dt: Get transactions from this date onwards (inclusive), format of yyyy-MM-dd, e.g. '2025-01-01'
        to_dt: Get transactions up to this date (inclusive), formtat of yyyy-MM-dd, e.g. '2025-01-01'
        partition_column: Name of column by which the data will be partitioned
        parallel: If set to True, Spark will launch multiple executors to fetch data and parallelize workloads.
        fetch_size: The number of rows to bet fetched from the database in a single round-trip
        num_partitions: The number of partitions to divide the data into

    Returns:
        Spark DataFrame of Postgres OPS transaction data
    """
    sql_query = read_file(
        "sources/sql/trans_extracts.sql"
    ).format(schema=get_postgres_env().schema, from_dt=from_dt, to_dt=to_dt)

    return read_pg(
        sql_query,
        parallel=parallel,
        partition_column=partition_column,
        lower_bound=from_dt,
        upper_bound=to_dt,
        num_partitions=num_partitions,
        fetch_size=fetch_size,
    )


def dqcheck_bronze_subsets(
    df: DataFrame,
    mappings: list={},
) -> Tuple[bool, DataFrame]:
    """
    Perform data quality checks on landed postgres ops transactions.
    """
    return (False, [])


def transforms_silver_transactions(
    path: str,
    from_dt: str,
    to_dt: str,
) -> DataFrame:
    """
    Extracted Postgres OPS data is transformed into silver data
    """

    return


def dqcheck_silver_subsets(
    df: DataFrame,
    mappings: list={},
) -> Tuple[bool, DataFrame]:
    """
    Perform data quality checks on landed postgres ops transactions.
    """
    return (False, [])


def transforms_gold_transactions(
    path: str,
    from_dt: str,
    to_dt: str,
) -> DataFrame:
    """
    Extracted Postgres OPS data is transformed into gold data
    """

    return


def dqcheck_gold_subsets(
    df: DataFrame,
    mappings: list={},
) -> Tuple[bool, DataFrame]:
    """
    Perform data quality checks on landed postgres ops transactions.
    """
    return (False, [])