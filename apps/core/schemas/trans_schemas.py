from pyspark.sql import types as T

ops_txn_schema = T.StructType(
    [
        T.StructField("CREATED_DATE", T.StringType()),
        T.StructField("TRANSACTION_ID", T.StringType()),
    ]
)