from core.utils import get_or_create_spark_session
from pipelines.ingest_transactions.ingest_transactions import (
    BronzeIngestTransPipeline,
    GoldIngestTransPipeline,
    SilverIngestTransPipeline,
)

# Variables
# run_dt = date.today().strftime("%Y-%m-%d")
run_dt = "2005-08-24"

if __name__ == "__main__":
    bronze = BronzeIngestTransPipeline(as_of_date=run_dt)
    silver = SilverIngestTransPipeline(as_of_date=run_dt)
    gold = GoldIngestTransPipeline(as_of_date=run_dt)
    bronze.run()
    silver.run()
    gold.run()
    spark = get_or_create_spark_session()
    spark.stop()
