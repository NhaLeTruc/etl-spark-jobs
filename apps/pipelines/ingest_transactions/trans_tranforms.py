# Externals
from typing import Tuple
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat, lit, sha2, struct

# Internals
from apps.core.conf.storage import DOCKER_ENV
from apps.core.mappings.oltp_to_olap_labels import ops_dwh_transactions_map
from apps.core.utils import read_module_file, cal_partition_dt
from apps.core.crud.postgres_ops import ops_read, ops_write


def extracts_bronze_transactions(
    from_dt: str,
    to_dt: str,
    partition_column: str,
    schema_name: str,
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
    sql_query = read_module_file(
        "crud/sql/trans_extracts.sql"
    ).format(
        schema_name= schema_name,
        partition_column=partition_column, 
        from_dt=from_dt, 
        to_dt=to_dt
    )

    partition_dt = cal_partition_dt(
        from_dt=from_dt,
        to_dt=to_dt,
        num_partitions=num_partitions,
    )

    return ops_read(
        sql_query,
        parallel=parallel,
        partition_column=partition_column,
        lower_bound=partition_dt[0],
        upper_bound=partition_dt[1],
        num_partitions=num_partitions,
        fetch_size=fetch_size,
    )


def transforms_silver_transactions(
    df: DataFrame,
    report_dt: str,
) -> DataFrame:
    """
    Bronze data is transformed into silver data through: 
        cleaning.
        business logics validating.
        structured as atomic transactions for easy analysis.
    """
    # 1. Cast all values in DataFrame as string
    # . Add report date
    df = df.select([col(c).cast("string") for c in df.columns]) \
            .withColumn("report_date", lit(report_dt))\

    return df


def transforms_gold_transactions(
    df: DataFrame,
    report_dt: str,
) -> DataFrame:
    """
    Silver data is transformed into gold data through:
        curating into dedicated business divsion's aggregated views.
            1. Cast all values in DataFrame as string.
            2. Remove horror films.
            3. Filter for customer segments in NYC and Saigon.
            4. Fill in NA value with DWH compliant string.
            5. Map columns' name to DWH compliant column names
            6. Add report date
    """
    df = df.select([col(c).cast("string") for c in df.columns]) \
            .filter(col("category_name") == "horror") \
            .filter(col("city").isin(["NYC","Saigon"])) \
            .fillna({"postal_code": "Not Available"}) \
            .select([col(c).alias(ops_dwh_transactions_map.get(c, c)) for c in df.columns]) \
            .withColumn("report_date", lit(report_dt))\

    return df
