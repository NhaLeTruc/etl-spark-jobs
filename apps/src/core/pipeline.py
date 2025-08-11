# Externals
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Tuple
from pyspark.sql import DataFrame, SparkSession

# Internals
from core.constants import DateTimeFormat
from core.utils import get_or_create_spark_session


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


    def enforced_dqc_checks(
        self,
        df: DataFrame,
        mappings: list={},
    ) -> Tuple[bool, DataFrame]:
        """
        
        """
        self._df_shape_check(df,mappings)
        self._df_size_check(df,mappings)
        self._df_null_check(df,mappings)
        self._df_keys_check(df,mappings)
        self._df_values_check(df,mappings)


    def _df_shape_check(
        self,
        df: DataFrame,
        mappings: list={},
    ) -> Tuple[bool, DataFrame]:
        """
        Check whether Spark DataFrame is of the right shape i.e. correct composite of columns.
        Return a DQC Pass/Fail bool and a KPIs report in form of a Spark DataFrame.
        """
        return False


    def _df_size_check(
        self,
        df: DataFrame,
        mappings: list={},
    ) -> Tuple[bool, DataFrame]:
        """
        Check whether Spark DataFrame is of the right size i.e. correct rowcounts.
        Return a DQC Pass/Fail bool and a KPIs report in form of a Spark DataFrame.
        """
        return False


    def _df_values_check(
        self,
        df: DataFrame,
        mappings: list={},
    ) -> Tuple[bool, DataFrame]:
        """
        Check whether each Spark DataFrame column has their expected values.
        Return a DQC Pass/Fail bool and a KPIs report in form of a Spark DataFrame.
        """
        return False


    def _df_keys_check(
        self,
        df: DataFrame,
        mappings: list={},
    ) -> Tuple[bool, DataFrame]:
        """
        Check whether Spark DataFrame has the right keys i.e. primary or composite keys are unique per record.
        Return a DQC Pass/Fail bool and a KPIs report in form of a Spark DataFrame.
        """
        return False


    def _df_null_check(
        self,
        df: DataFrame,
        mappings: list={},
    ) -> Tuple[bool, DataFrame]:
        """
        Check whether each Spark DataFrame column has their acceptable levels of null values.
        Return a DQC Pass/Fail bool and a KPIs report in form of a Spark DataFrame.
        """
        return False
    