import logging
import datetime

logger = logging.getLogger(__name__)

DEFAULT_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"
DEFAULT_DATE_FORMAT = "yyyy-MM-dd"
JAVA_PYTHON_TS_FORMAT_CONVERTER = {

    # TIMESTAMP
    "yyyy-MM-dd HH:mm:ss": "%Y-%m-%d %H:%M:%S",
    "yyyy/MM/dd HH:mm:ss": "%Y/%m/%d %H:%M:%S",
    "dd/MM/yyyy HH:mm:ss": "%d/%m/%Y %H:%M:%S",
    "dd_MM_yyyy HH:mm:ss": "%d_%m_%Y %H:%M:%S",

    "dd-MM-yyyy HH.mm.ss": "%d/%m/%Y %H.%M.%S"

}

JAVA_PYTHON_DT_FORMAT_CONVERTER = {

    # DATE
    "yyyy-MM-dd": "%Y-%m-%d",
    "yyyy/MM/dd": "%Y/%m/%d",
    "yyyy_MM_dd": "%Y_%m_%d",

    "dd-MM-yyyy": "%d-%m-%Y",
    "dd/MM/yyyy": "%d/%m/%Y",
    "dd_MM_yyyy": "%d_%m_%Y",

    "dd-MM-yy": "%d-%m-%y",
}


def to_datetime(timestamp: str, java_timestamp_format: str = DEFAULT_TIMESTAMP_FORMAT) -> datetime.datetime:

    try:

        python_ts_format: str = JAVA_PYTHON_TS_FORMAT_CONVERTER[java_timestamp_format]
        return datetime.datetime.strptime(timestamp, python_ts_format)

    except KeyError:

        logger.error(f"Undefined Java timestamp format: {java_timestamp_format}. Update the dict with such format")
        raise KeyError(java_timestamp_format)


def to_date(date: str, java_date_format: str = DEFAULT_DATE_FORMAT) -> datetime.date:

    try:

        python_dt_format: str = JAVA_PYTHON_DT_FORMAT_CONVERTER[java_date_format]
        return datetime.datetime.strptime(date, python_dt_format).date()

    except KeyError:

        logger.error(f"Undefined Java timestamp format: {java_date_format}. Update the dict with such format")
        raise KeyError(java_date_format)

