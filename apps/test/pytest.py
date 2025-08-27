import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark_session():
    """Provides a shared SparkSession for tests."""
    spark = SparkSession.builder \
        .appName("PySpark Test") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()