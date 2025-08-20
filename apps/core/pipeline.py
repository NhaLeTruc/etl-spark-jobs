# Externals
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Tuple, Dict
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit

# Internals
from apps.core.constants import DateTimeFormat
from apps.core.utils import get_or_create_spark_session


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
        df: DataFrame={},        
        mappings: Dict={},
    ) -> Tuple[bool, DataFrame]:
        """
        Perform highly reccommended data quality tests
        Return a quality check boolean flag and a Spark DataFrame report
        Report items:
            Measurement, Table, Columns Unit, Upper, Lower, Measured, Remark, Note
            No. of columns, tableA, All, Count,  10,  10,   10,     Pass,    Great
            Unexpected values, tableA, columnB, Count, 0,  0,   1,   Failed,  Attention Required!
        """
        go_ahead = False
        combined_reports = None
        results = [ 
            self._df_shape_check(df,mappings),
            self._df_size_check(df,mappings),
            self._df_null_check(df,mappings),
            self._df_keys_check(df,mappings),
            self._df_values_check(df,mappings),
        ]

        for flag, report in results:
            go_ahead += flag
            if combined_reports:
                combined_reports = combined_reports.union(report)
            else:
                combined_reports = report
        
        combined_reports = combined_reports.withColumn("Report_Date", lit(self.as_of_date_fmt))

        return go_ahead, combined_reports


    def _df_shape_check(
        self,
        df: DataFrame={},
        mappings: Dict={},
    ) -> Tuple[bool, DataFrame]:
        """
        Check whether Spark DataFrame is of the right shape i.e. correct composite of columns.
        Return a DQC Pass/Fail bool and a KPIs report in form of a Spark DataFrame.
        """
        return False, df


    def _df_size_check(
        self,
        df: DataFrame={},
        mappings: Dict={},
    ) -> Tuple[bool, DataFrame]:
        """
        Check whether Spark DataFrame is of the right size i.e. correct rowcounts.
        Return a DQC Pass/Fail bool and a KPIs report in form of a Spark DataFrame.
        """
        return False, df


    def _df_values_check(
        self,
        df: DataFrame={},
        mappings: Dict={},
    ) -> Tuple[bool, DataFrame]:
        """
        Check whether each Spark DataFrame column has their expected values.
        Return a DQC Pass/Fail bool and a KPIs report in form of a Spark DataFrame.
        """
        return False, df


    def _df_keys_check(
        self,
        df: DataFrame={},
        mappings: Dict={},
    ) -> Tuple[bool, DataFrame]:
        """
        Check whether Spark DataFrame has the right keys i.e. primary or composite keys are unique per record.
        Return a DQC Pass/Fail bool and a KPIs report in form of a Spark DataFrame.
        """
        return False, df


    def _df_null_check(
        self,
        df: DataFrame={},
        mappings: Dict={},
    ) -> Tuple[bool, DataFrame]:
        """
        Check whether each Spark DataFrame column has their acceptable levels of null values.
        Return a DQC Pass/Fail bool and a KPIs report in form of a Spark DataFrame.
        """
        return False, df
    