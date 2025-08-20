"""
main file of pipeline object "ingest_transactions"
"""
# Externals
from datetime import timedelta

# Internals
from apps.core.constants import DateTimeFormat
from apps.core.pipeline import BaseDataPipeline
from apps.core.crud.minio_lake import minio_create
from apps.core.mappings.ingest_transactions_dqc import (
    bronze_dqc_json,
    silver_dqc_json,
    gold_dqc_json,
)
from apps.pipelines.ingest_transactions.trans_tranforms import (
    extracts_bronze_transactions,
    dqcheck_bronze_subsets,
    transforms_gold_transactions,
    dqcheck_gold_subsets,
    transforms_silver_transactions,
    dqcheck_silver_subsets,
)


class BronzeIngestTransPipeline(BaseDataPipeline):
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
        
        minio_create(df)
        
        self.enforced_dqc_checks(df,bronze_dqc_json)

        dqcheck_bronze_subsets(df)


class SilverIngestTransPipeline(BaseDataPipeline):
    def __init__(self, as_of_date: str, lookback_days: int = 1):
        super().__init__(as_of_date)
        self.lookback_days = lookback_days


    def run(self):
        to_dt = self.as_of_date_fmt()
        from_dt = (self.as_of_date - timedelta(days=int(self.lookback_days))).strftime(
            DateTimeFormat.ISO_DATE_FMT.value
        )

        df = transforms_silver_transactions(
            from_dt=from_dt,
            to_dt=to_dt,
            partition_column="CREATED_DATE",
            num_partitions=10,
        )
        
        minio_create(df)
        
        self.enforced_dqc_checks(df,silver_dqc_json)

        dqcheck_silver_subsets(df)


class GoldIngestTransPipeline(BaseDataPipeline):
    def __init__(self, as_of_date: str, lookback_days: int = 1):
        super().__init__(as_of_date)
        self.lookback_days = lookback_days


    def run(self):
        to_dt = self.as_of_date_fmt()
        from_dt = (self.as_of_date - timedelta(days=int(self.lookback_days))).strftime(
            DateTimeFormat.ISO_DATE_FMT.value
        )

        df = transforms_gold_transactions(
            from_dt=from_dt,
            to_dt=to_dt,
            partition_column="CREATED_DATE",
            num_partitions=10,
        )
        
        minio_create(df)
        
        self.enforced_dqc_checks(df,gold_dqc_json)

        dqcheck_gold_subsets(df)
