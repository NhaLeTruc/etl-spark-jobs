"""
main file of pipeline object "ingest_transactions"
"""
# Externals
from datetime import timedelta
from pyspark.sql.functions import col, lit, struct, to_json

# Internals
from core.conf.storage import MINIO_DATA_PATH
from core.constants import DateTimeFormat
from core.pipeline import BaseDataPipeline
from core.sources.minio_lake import persist_minio
from core.mappings.dqc_ingest_transactions import bronze_dqc_json
from pipelines.ingest_transactions.trans_tranforms import (
    extracts_bronze_transactions,
    dqcheck_bronze_transactions,
    transforms_gold_transactions,
    transforms_silver_transactions,
)


class ExtractTransactionsPipeline(BaseDataPipeline):
    def __init__(self, as_of_date: str, lookback_days: int = 1):
        super().__init__(as_of_date)
        self.lookback_days = lookback_days


    def run(self):
        to_dt = self.as_of_date_fmt()
        from_dt = (self.as_of_date - timedelta(days=int(self.lookback_days))).strftime(
            DateTimeFormat.ISO_DATE_FMT.value
        )

        df = extracts_bronze_transactions(
            from_dt=from_dt,
            to_dt=to_dt,
            partition_column="CREATED_DATE",
            num_partitions=10,
        )
        
        persist_minio(df)
        
        self.df_shape_check(df,bronze_dqc_json)
        self.df_size_check(df,bronze_dqc_json)
        self.df_null_check(df,bronze_dqc_json)
        self.df_keys_check(df,bronze_dqc_json)
        self.df_values_check(df,bronze_dqc_json)
