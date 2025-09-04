
from datetime import date

from apps.core.conf.jdbc import JdbcConfig
from apps.core.crud.postgres_ops import ops_read
from apps.core.mappings.oltp_to_olap_labels import ops_dwh_transactions_map
from apps.core.utils import cal_partition_dt, read_module_file
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, regexp_replace, to_date


def extracts_bronze_transactions(
    from_dt: str,
    to_dt: str,
    partition_column: str,
    schema_name: str,
    config: JdbcConfig,
    num_partitions: int = 10,
    parallel: bool = True,
    fetch_size: int = 10_000,
) -> DataFrame:
    """
    Run SQL script to fetch postgres ops transactions.

    Args:
        from_dt: Get transactions from this date onwards (inclusive), format of yyyy-MM-dd, e.g. '2025-01-01'
        to_dt: Get transactions up to this date (inclusive), format of yyyy-MM-dd, e.g. '2025-01-01'
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

    res_df = ops_read(
        sql_query,
        config=config,
        parallel=parallel,
        partition_column=partition_column,
        lower_bound=partition_dt[0],
        upper_bound=partition_dt[1],
        num_partitions=num_partitions,
        fetch_size=fetch_size,
    )

    return res_df.withColumn("partition_date", to_date(col("rental_date")))

def transforms_silver_transactions(
    df: DataFrame,
    report_dt: str,
) -> DataFrame:
    """
    Bronze data is transformed into silver data through:
        cleaning.
        business logics validating.
        structured as atomic transactions for easy analysis.
            1. Remove assumed corrupted records.
            2. Remove has not been released films.
            3. Remove test film titles, all of which has special characters.
            4. Add report date.
            5. Cast all values in DataFrame as string.
    """
    return df.where(col("activebool") == col("active")) \
            .where(col("release_year") < date.today().year) \
            .where(regexp_replace(col("title"), "\\s+", "").rlike("^[a-zA-Z0-9]{3,60}$")) \
            .select([col(c).cast("string") for c in df.columns]) \
            .withColumn("report_date", lit(report_dt)) \



def transforms_gold_transactions(
    df: DataFrame,
) -> DataFrame:
    """
    Silver data is transformed into gold data through:
        curating into dedicated business divsion's aggregated views.
            1. Remove film which length is shorter than 90 minutes.
            2. Remove horror films.
            3. Keep only customers in relevant cities.
            4-5. Fill in NA and blank values with DWH compliant string.
            6. Map columns' name to DWH compliant column names.
            7. Cast all values in DataFrame as string.
            8. Remove unwanted columns.
            9. Relabel lakehouse's namings with datawarehouse's.
    Arg:
        df: Spark Dataframe input
    Return:
        A Spark Dataframe
    """
    df = df.where(col("length") > 90) \
            .where(col("category_name") != "Horror") \
            .where(col("city").isin(["Aurora", "London", "Saint-Denis", "Cape Coral", "Molodetno", "Tanza", "Changzhou", "Ourense (Orense)"])) \
            .replace("", None) \
            .fillna({"postal_code": "Not Applicable", "address2": "Not Applicable"}) \
            .select([col(c).cast("string") for c in df.columns]) \
            .drop(
                "last_rental_update",
                "last_customer_update",
                "last_address_update",
                "last_inventory_update",
            )

    return df.select([col(c).alias(ops_dwh_transactions_map.get(c, c)) for c in df.columns])
