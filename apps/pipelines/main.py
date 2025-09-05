from apps.core.utils import get_or_create_spark_session
from apps.pipelines.ingest_transactions.ingest_transactions import (
    BronzeIngestTransPipeline,
    GoldIngestTransPipeline,
    SilverIngestTransPipeline,
)


def main():
    """
    Run whole pipeline from bronze to gold
    """
    # run_dt = date.today().strftime("%Y-%m-%d")
    run_dt = "2005-08-24"

    bronze = BronzeIngestTransPipeline(as_of_date=run_dt)
    silver = SilverIngestTransPipeline(as_of_date=run_dt)
    gold = GoldIngestTransPipeline(as_of_date=run_dt)
    bronze.run()
    silver.run()
    gold.run()
    spark = get_or_create_spark_session()
    spark.stop()

if __name__ == "__main__":
    main()
