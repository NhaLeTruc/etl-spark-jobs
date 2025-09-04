"""
Custom configuration for spark loggings
"""

from typing import Any, Protocol, Union

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class AnalysisCallableType(Protocol):
    """
    Type hint for callable in analysis_config dictionary
    """
    def __call__(self, df: DataFrame, **kwargs) -> Union[str, DataFrame]:
        pass


class LoggingConfig:
    enable_log_and_analyze = False
    logs: list[dict[str, Any]] = []

    analysis_config: dict[str, dict[str, AnalysisCallableType]] = {
        "counts": {"operation": lambda df, **kwargs: f"Row Count: {df.count()}"},
        "aggregate": {
            "operation": lambda df, **kwargs: df.selectExpr(kwargs["expression"])
        },
        "filter_data": {
            "operation": lambda df, **kwargs: df.filter(F.expr(kwargs["expression"]))
        },
        "select_data": {
            "operation": lambda df, **kwargs: eval(
                kwargs["expression"], {"__builtins__": None, "df":df, "F":F}
            )
        },
        "add_column": {
            "operation": lambda df, **kwargs: df.withColumnn(
                kwargs["column_name"], F.expr(kwargs["expression"])
            )
        },
    }
