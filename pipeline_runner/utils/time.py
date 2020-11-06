import logging
import datetime


class TimeUtils:

    _logger = logging.getLogger(__name__)

    _DEFAULT_DATE_FORMAT = "yyyy-MM-dd"
    _DEFAULT_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"
    _JAVA_TO_PYTHON_FORMAT = {

        # Date
        "yyyy-MM-dd": "%Y-%m-%d",
        "yyyy/MM/dd": "%Y/%m/%d",
        "yyyy_MM_dd": "%Y_%m_%d",

        "dd-MM-yyyy": "%d-%m-%Y",
        "dd/MM/yyyy": "%d/%m/%Y",
        "dd_MM_yyyy": "%d_%m_%Y",

        "dd-MM-yy": "%d-%m-%y",

        # Timestamp

        "yyyy-MM-dd HH:mm:ss": "%Y-%m-%d %H:%M:%S",
        "yyyy/MM/dd HH:mm:ss": "%Y/%m/%d %H:%M:%S",
        "dd/MM/yyyy HH:mm:ss": "%d/%m/%Y %H:%M:%S",
        "dd_MM_yyyy HH:mm:ss": "%d_%m_%Y %H:%M:%S",
        "dd-MM-yyyy HH.mm.ss": "%d/%m/%Y %H.%M.%S"
    }

    @classmethod
    def default_date_format(cls) -> str:
        return cls._DEFAULT_DATE_FORMAT

    @classmethod
    def default_timestamp_format(cls) -> str:
        return cls._DEFAULT_TIMESTAMP_FORMAT

    @classmethod
    def to_python_format(cls, java_format: str) -> str:

        return cls._JAVA_TO_PYTHON_FORMAT[java_format]

    @classmethod
    def to_datetime(cls, timestamp: str, java_timestamp_format: str = None) -> datetime.datetime:

        try:

            java_format = java_timestamp_format if java_timestamp_format else cls._DEFAULT_TIMESTAMP_FORMAT
            python_ts_format: str = cls.to_python_format(java_format)
            return datetime.datetime.strptime(timestamp, python_ts_format)

        except KeyError:

            cls._logger.error(f"Undefined Java timestamp format: '{java_timestamp_format}'. Update the dict with such format")
            raise KeyError(java_timestamp_format)

    @classmethod
    def to_date(cls, date: str, java_date_format: str = None) -> datetime.date:

        try:

            java_format = java_date_format if java_date_format else cls._DEFAULT_DATE_FORMAT
            python_dt_format: str = cls.to_python_format(java_format)
            return datetime.datetime.strptime(date, python_dt_format).date()

        except KeyError:

            cls._logger.error(f"Undefined Java date format: '{java_date_format}'. Update the dict with such format")
            raise KeyError(java_date_format)
