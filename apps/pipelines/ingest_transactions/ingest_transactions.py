"""
main file of pipeline object "ingest_transactions"
"""
# Externals
from datetime import date, timedelta

# Internals
from apps.core.constants import DateTimeFormat
from apps.core.pipeline import BaseDataPipeline
from apps.core.crud.minio_lake import minio_write, minio_read
from apps.core.conf.storage import MINIO_BUCKETS
from apps.core.mappings.ingest_transactions_dqc import (
    bronze_dqc_json,
    silver_dqc_json,
    gold_dqc_json,
)
from apps.pipelines.ingest_transactions.trans_tranforms import (
    extracts_bronze_transactions,
    transforms_gold_transactions,
    transforms_silver_transactions,
)

# Variables
bucket_lake = MINIO_BUCKETS["ops"]["lake"] + "/OPS/rental_bronze"
bucket_house = MINIO_BUCKETS["ops"]["dwh"] + "/OPS/rental_silver"
bucket_lakehouse = MINIO_BUCKETS["ops"]["lakehouse"] + "/OPS/rental_gold"
ops_schema = "postgres"
partition_dt = "rental_date"


#########################################################################
## BRONZE

class BronzeIngestTransPipeline(BaseDataPipeline):

    def __init__(
        self, 
        as_of_date: str = date.today(), 
        lookback_days: int = 365,
    ):
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
            schema_name=ops_schema,
            partition_column=partition_dt,
            num_partitions=5,
        )
        
        minio_write(
            df=df,
            path=bucket_lake,
            partition_cols=partition_dt,
        )
        
        self.enforced_dqc_checks(df,bronze_dqc_json)


#########################################################################
## SILVER

class SilverIngestTransPipeline(BaseDataPipeline):

    def __init__(
        self, 
        as_of_date: str = date.today(),
    ):
        super().__init__(as_of_date)

    def run(self):
        report_dt = self.as_of_date_fmt()

        df = minio_read(
            path=bucket_lake,
            table_name="rental_bronze",
        )

        df = transforms_silver_transactions(
            df=df,
            report_dt=report_dt,
        )
        
        minio_write(
            df=df,
            path=bucket_lakehouse,
            partition_cols=self.partition_column,
        )
                
        self.enforced_dqc_checks(df,silver_dqc_json)


#########################################################################
## GOLD

class GoldIngestTransPipeline(BaseDataPipeline):

    def __init__(
        self, 
        as_of_date: str = date.today(),
    ):
        super().__init__(as_of_date)


    def run(self):
        report_dt = self.as_of_date_fmt()

        df = minio_read(
            path=bucket_lake,
            table_name="rental_silver",
        )

        df = transforms_gold_transactions(
            df=df,
            report_dt=report_dt,
        )
        
        minio_write(
            df=df,
            path=bucket_house,
            partition_cols=partition_dt,
        )
        
        self.enforced_dqc_checks(df,gold_dqc_json)
