
from abc import ABC, abstractmethod
from datetime import datetime

from core.constants import DateTimeFormat
from core.utils import get_or_create_spark_session
from pyspark.sql import SparkSession


class BaseDataPipeline(ABC):
    def __init__(self, as_of_date: str):
        """
        Initialize the pipeline
        Arg:
            as_of_date: in format yyyy=MM-dd, e.g. '2025-01-01'
        """
        super().__init__()
        self._as_of_date = datetime.strptime(as_of_date, "%Y-%m-%d")


    @property
    def as_of_date(self) -> datetime:
        """
        as_of_date is the date this pipeline was started
        """
        return self._as_of_date


    def as_of_date_fmt(self, fmt_str: str = DateTimeFormat.ISO_DATE_FMT.value) -> str:
        """
        as_of_date in yyyy-MM-dd format, e.g. '2025-01-01'
        """
        return self._as_of_date.strftime(fmt_str)


    @property
    def spark(self) -> SparkSession:
        """
        SparkSession getter method
        """
        return get_or_create_spark_session()


    @abstractmethod
    def run(self):
        """
        Entrypoint for starting pipeline.
        """
