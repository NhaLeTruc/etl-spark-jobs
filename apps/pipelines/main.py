import os.path
import sys

from core.utils import get_or_create_spark_session
from pipelines.ingest_transactions.ingest_transactions import (
    BronzeIngestTransPipeline,
    GoldIngestTransPipeline,
    SilverIngestTransPipeline,
)

# Variables
# run_dt = date.today().strftime("%Y-%m-%d")
run_dt = "2005-08-24"

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname("apps/core/utils.py"), '..'))
sys.path.append(os.path.join(os.path.dirname("apps/test/test_utils.py"), '..'))

if __name__ == "__main__":
    bronze = BronzeIngestTransPipeline(as_of_date=run_dt)
    silver = SilverIngestTransPipeline(as_of_date=run_dt)
    gold = GoldIngestTransPipeline(as_of_date=run_dt)
    bronze.run()
    silver.run()
    gold.run()
    spark = get_or_create_spark_session()
    spark.stop()
