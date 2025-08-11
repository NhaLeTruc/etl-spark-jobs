"""
Repository for ingest_transactions data quality checks 
"""

# Externals
from pyspark.sql import DataFrame

# Internals


def df_shape_check(
        df: DataFrame,
        mappings: object,
) -> bool:
    """
    Check whether Spark DataFrame is of the right shape i.e. correct composite of columns.
    """
    return False


def df_size_check(
        df: DataFrame,
        mappings: object,
) -> bool:
    """
    Check whether Spark DataFrame is of the right size i.e. correct rowcounts.
    """
    return False


def df_values_check(
        df: DataFrame,
        mappings: object,
) -> bool:
    """
    Check whether Spark DataFrame has the right values i.e. columns values are expected.
    """
    return False