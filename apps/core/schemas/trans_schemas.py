from pyspark.sql import types as T

ops_txn_schema = T.StructType(
    [
        T.StructField("CREATED_DATE", T.StringType(), False, metadata={"format": "%Y-%m-%d", "validation_rule": "CREATED_DATE > 2025-01-01"}),
        T.StructField("TRANSACTION_ID", T.StringType()),
    ]
)