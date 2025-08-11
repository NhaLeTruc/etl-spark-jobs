# Unlikely to change values

from enum import Enum, unique

class DateTimeFormat(Enum):
    """
    Standard date time formats
    """
    ISO_DATE_FMT = "%Y-%m-%d"
    CUR_DATE_TIME_FMT = "%Y-%m-%d %H:%M:%S"
    