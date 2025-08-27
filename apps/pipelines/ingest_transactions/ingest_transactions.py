"""
main file of pipeline object "ingest_transactions"
"""
# Externals
from datetime import date, timedelta

# Internals
from apps.core.constants import DateTimeFormat
from apps.core.pipeline import BaseDataPipeline
from apps.core.crud.minio_lake import minio_write, minio_read
from apps.core.crud.postgres_ops import ops_write
from apps.core.conf.storage import MINIO_BUCKETS, DOCKER_ENV
from apps.core.conf.jdbc import DockerEnvJdbcConfig
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
ops_config = DockerEnvJdbcConfig(config=DOCKER_ENV.get("postgres"))
ops_schema = "dvdrental.public"
partition_dt = "rental_date"
# run_dt = date.today().strftime("%Y-%m-%d")
run_dt = "2006-02-14"


#########################################################################
## BRONZE

class BronzeIngestTransPipeline(BaseDataPipeline):

    def __init__(
        self, 
        as_of_date: str = run_dt, 
        lookback_days: int = 100,
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
        as_of_date: str = run_dt,
    ):
        super().__init__(as_of_date)

    def run(self):

        df = minio_read(
            path=bucket_lake,
            table_name="rental_bronze",
        )

        df = transforms_silver_transactions(
            df=df,
            report_dt=self.as_of_date_fmt(),
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
        as_of_date: str = run_dt,
    ):
        super().__init__(as_of_date)


    def run(self):

        df = minio_read(
            path=bucket_lake,
            table_name="rental_silver",
        )

        df = transforms_gold_transactions(
            df=df,
            report_dt=self.as_of_date_fmt(),
        )
        
        self.enforced_dqc_checks(df,gold_dqc_json)

        minio_write(
            df=df,
            path=bucket_house,
            partition_cols=partition_dt,
        )

        ops_write(
            dbtable="rental_gold",
            df=df,
            config=ops_config,
        )
