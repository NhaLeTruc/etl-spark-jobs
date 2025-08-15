"""
Unlikely to change values
"""
# Externals
from enum import Enum, unique


class DateTimeFormat(Enum):
    """
    Accepted datetime formats
    """
    ISO_DATE_FMT = "%Y-%m-%d"
    CUR_DATE_TIME_FMT = "%Y-%m-%d %H:%M:%S"
    

class StorageFormats(Enum):
    """
    Accepted data file formats
    """
    MINIO_STORAGE_FORMAT = "parquet"