import logging
from datetime import datetime, date
from typing import Union, Callable


class TimeUtils:

    _logger = logging.getLogger(__name__)

    _JAVA_DEFAULT_DATE_FORMAT = "yyyy-MM-dd"
    _JAVA_DEFAULT_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"
    _JAVA_DEFAULT_CORRUPT_FORMAT = "dd-MM-yy"
    _JAVA_TO_PYTHON_FORMAT = {

        # Date
        _JAVA_DEFAULT_DATE_FORMAT: "%Y-%m-%d",
        "yyyy/MM/dd": "%Y/%m/%d",
        "yyyy_MM_dd": "%Y_%m_%d",

        "dd-MM-yyyy": "%d-%m-%Y",
        "dd/MM/yyyy": "%d/%m/%Y",
        "dd_MM_yyyy": "%d_%m_%Y",

        _JAVA_DEFAULT_CORRUPT_FORMAT: "%d-%m-%y",

        # Timestamp

        _JAVA_DEFAULT_TIMESTAMP_FORMAT: "%Y-%m-%d %H:%M:%S",
        "yyyy/MM/dd HH:mm:ss": "%Y/%m/%d %H:%M:%S",
        "dd/MM/yyyy HH:mm:ss": "%d/%m/%Y %H:%M:%S",
        "dd_MM_yyyy HH:mm:ss": "%d_%m_%Y %H:%M:%S",
        "dd-MM-yyyy HH.mm.ss": "%d/%m/%Y %H.%M.%S"
    }

    @classmethod
    def _check_format(cls, date_str: str, java_format: str, time_function: Callable[[str, str], Union[datetime, date]]):

        try:
            time_function(date_str, java_format)
        except ValueError:
            return False
        else:
            return True

    @classmethod
    def java_default_dt_format(cls) -> str:
        return cls._JAVA_DEFAULT_DATE_FORMAT

    @classmethod
    def java_default_ts_format(cls) -> str:
        return cls._JAVA_DEFAULT_TIMESTAMP_FORMAT

    @classmethod
    def java_default_corrupt_format(cls) -> str:
        return cls._JAVA_DEFAULT_CORRUPT_FORMAT

    @classmethod
    def to_python_format(cls, java_format: str) -> str:
        return cls._JAVA_TO_PYTHON_FORMAT[java_format]

    @classmethod
    def to_datetime(cls, timestamp: str, java_format: str = None) -> datetime:

        java_format = java_format if java_format else cls._JAVA_DEFAULT_TIMESTAMP_FORMAT
        python_ts_format: str = cls.to_python_format(java_format)
        return datetime.strptime(timestamp, python_ts_format)

    @classmethod
    def to_date(cls, date_: str, java_format: str = None) -> date:

        date_format = java_format if java_format else cls._JAVA_DEFAULT_DATE_FORMAT
        return cls.to_datetime(date_, date_format).date()

    @classmethod
    def format(cls, date_or_datetime: Union[datetime, date], java_format: str = None) -> Union[str, None]:

        if date_or_datetime is not None:

            j_format: str = java_format if java_format else \
                (cls._JAVA_DEFAULT_TIMESTAMP_FORMAT if isinstance(date_or_datetime, datetime) else cls._JAVA_DEFAULT_DATE_FORMAT)

            return date_or_datetime.strftime(cls.to_python_format(j_format))

        else:

            return None

    @classmethod
    def has_date_format(cls, date_str: str) -> bool:

        return cls._check_format(date_str, cls._JAVA_DEFAULT_DATE_FORMAT, cls.to_date)

    @classmethod
    def has_datetime_format(cls, datetime_str: str) -> bool:

        return cls._check_format(datetime_str, cls._JAVA_DEFAULT_TIMESTAMP_FORMAT, cls.to_datetime)
