from apps.pipelines.ingest_transactions.ingest_transactions import (
    BronzeIngestTransPipeline,
    SilverIngestTransPipeline,
    GoldIngestTransPipeline,
)
from apps.core.utils import get_or_create_spark_session

if __name__ == "__main__":
    bronze = BronzeIngestTransPipeline()
    silver = SilverIngestTransPipeline()
    gold = GoldIngestTransPipeline()
    bronze.run()
    silver.run()
    gold.run()
    spark = get_or_create_spark_session()
    spark.stop()