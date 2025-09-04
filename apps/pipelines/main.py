from apps.core.utils import get_or_create_spark_session
from apps.pipelines.ingest_transactions.ingest_transactions import (
    BronzeIngestTransPipeline,
    GoldIngestTransPipeline,
    SilverIngestTransPipeline,
)

if __name__ == "__main__":
    bronze = BronzeIngestTransPipeline()
    silver = SilverIngestTransPipeline()
    gold = GoldIngestTransPipeline()
    bronze.run()
    silver.run()
    gold.run()
    spark = get_or_create_spark_session()
    spark.stop()
