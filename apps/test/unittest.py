import unittest
from pyspark.sql import SparkSession

class MyPySparkTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Set up a shared SparkSession for all tests in this class."""
        cls.spark = SparkSession.builder \
            .appName("PySpark Unit Test") \
            .master("local[*]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        """Stop the shared SparkSession after all tests in this class are run."""
        cls.spark.stop()

    def test_my_pyspark_function(self):
        # Your test logic here, using self.spark
        data = [("Alice", 1), ("Bob", 2)]
        df = self.spark.createDataFrame(data, ["name", "age"])
        
        # Example: Perform a simple transformation
        result_df = df.filter(df.age > 1)
        
        # Assertions
        self.assertEqual(result_df.count(), 1)
        self.assertEqual(result_df.collect()[0]["name"], "Bob")