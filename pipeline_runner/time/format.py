import logging
import datetime

logger = logging.getLogger(__name__)

DEFAULT_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"
DEFAULT_DATE_FORMAT = "yyyy-MM-dd"
JAVA_TO_PYTHON_FORMAT = {

    # DATE
    "yyyy-MM-dd": "%Y-%m-%d",
    "yyyy/MM/dd": "%Y/%m/%d",
    "yyyy_MM_dd": "%Y_%m_%d",

    "dd-MM-yyyy": "%d-%m-%Y",
    "dd/MM/yyyy": "%d/%m/%Y",
    "dd_MM_yyyy": "%d_%m_%Y",

    "dd-MM-yy": "%d-%m-%y",

    # TIME
    "HH:mm:ss": "%H:%M:%S",

    # TIMESTAMP
    "yyyy-MM-dd HH:mm:ss": "%Y-%m-%d %H:%M:%S",
    "yyyy/MM/dd HH:mm:ss": "%Y/%m/%d %H:%M:%S",
    "dd/MM/yyyy HH:mm:ss": "%d/%m/%Y %H:%M:%S",
    "dd_MM_yyyy HH:mm:ss": "%d_%m_%Y %H:%M:%S",

    "dd-MM-yyyy HH.mm.ss": "%d/%m/%Y %H.%M.%S"

}


def to_datetime(date: str, java_date_format: str) -> datetime.datetime:

    try:

        return datetime.datetime.strptime(date, JAVA_TO_PYTHON_FORMAT[java_date_format])

    except KeyError:

        logger.error(f"Undefined Java date format: {java_date_format}. Update the dict with such format")
        raise KeyError(java_date_format)
